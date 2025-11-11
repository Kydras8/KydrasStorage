using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading.Tasks;
using KydrasStorage.Core.Interfaces;
using KydrasStorage.Core.Models;
using Microsoft.Extensions.Logging;

namespace KydrasStorage.Core.Services;

public class FilePoolService : IFilePoolService
{
    private readonly ILogger<FilePoolService> _logger;
    private readonly ConcurrentDictionary<Guid, StoragePool> _pools = new();

    private static string IndexPath =>
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "KydrasStorage", "kydras.db");

    public FilePoolService(ILogger<FilePoolService> logger) => _logger = logger;

    public Task<StoragePool> CreatePoolAsync(string name, List<string> drivePaths, PoolType type)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Pool name required", nameof(name));
        if (drivePaths is null || drivePaths.Count == 0) throw new ArgumentException("At least one drive path is required", nameof(drivePaths));

        _logger.LogInformation("Creating storage pool: {Name} with {Count} drives", name, drivePaths.Count);

        var drives = drivePaths.Select(CreatePoolDrive).ToList();

        var mp = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? Path.Combine("K:\\", name)
            : Path.Combine(Path.DirectorySeparatorChar + "pools", name);

        var pool = new StoragePool
        {
            Name = name,
            Type = type,
            MountPoint = mp,
            Drives = drives
        };

        _pools[pool.Id] = pool;
        _logger.LogInformation("Pool created: {PoolId}", pool.Id);

        return Task.FromResult(pool);
    }

    // ------- WRITE: Two-phase Commit -------
    public async Task<bool> WriteFileAsync(Guid poolId, string relativePath, Stream content)
    {
        if (!_pools.TryGetValue(poolId, out var pool))
        {
            _logger.LogWarning("Pool not found: {PoolId}", poolId);
            return false;
        }

        relativePath = SanitizeRelativePath(relativePath);
        var idx = new SqliteMetadataIndex(IndexPath);

        var rule = ResolveRule(pool, relativePath);
        var dupLevel = Math.Max(1, rule?.DuplicationLevel ?? 1);

        var size = content.CanSeek ? content.Length : 0L;
        var candidates = SelectEligibleDrivesWeighted(pool, relativePath, size, rule).ToList();
        var targets = candidates.Take(dupLevel).ToList();
        if (targets.Count < dupLevel)
            throw new InvalidOperationException($"Need {dupLevel} healthy drives; found {targets.Count}.");

        var temps = new List<(PoolDrive drive, string tempPath, string finalPath, string hash, long bytes)>();
        foreach (var d in targets)
        {
            var final = Path.Combine(d.RootPath, relativePath);
            var dir = Path.GetDirectoryName(final);
            if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

            var temp = final + "." + Guid.NewGuid().ToString("N") + ".2pc";
            if (content.CanSeek) content.Seek(0, SeekOrigin.Begin);
            await using (var fs = File.Create(temp))
                await content.CopyToAsync(fs);

            var hash = ComputeFileSha256(temp);
            var bytes = new FileInfo(temp).Length;
            temps.Add((d, temp, final, hash, bytes));
        }

        var distinct = temps.Select(t => t.hash).Distinct(StringComparer.OrdinalIgnoreCase).Count();
        if (distinct != 1)
        {
            foreach (var t in temps) TryDelete(t.tempPath);
            throw new IOException("Replica integrity verification failed in Phase 1.");
        }

        foreach (var t in temps)
        {
            AtomicReplace(t.tempPath, t.finalPath);
            UpdateDriveSpace(t.drive);
            idx.UpsertReplica(poolId, relativePath, t.drive.RootPath, t.bytes, t.hash, DateTime.UtcNow);
            _logger.LogInformation("2PC promote: {Path}", t.finalPath);
        }

        return true;
    }

    // ------- READ: Self-healing -------
    public Task<Stream?> ReadFileAsync(Guid poolId, string relativePath)
    {
        if (!_pools.TryGetValue(poolId, out var pool)) return Task.FromResult<Stream?>(null);

        relativePath = SanitizeRelativePath(relativePath);
        var idx = new SqliteMetadataIndex(IndexPath);

        var replicas = new List<(PoolDrive d, string full, bool exists, string? hash)>();
        foreach (var d in pool.Drives)
        {
            var full = Path.Combine(d.RootPath, relativePath);
            if (File.Exists(full))
            {
                string? h = null;
                try { h = ComputeFileSha256(full); } catch { }
                replicas.Add((d, full, true, h));
            }
            else
            {
                replicas.Add((d, full, false, null));
            }
        }

        var meta = idx.GetReplicas(poolId, relativePath);
        string? expected = meta.Select(m => m.sha256).FirstOrDefault();
        var source = replicas.FirstOrDefault(r => r.exists && r.hash != null && (expected == null || r.hash.Equals(expected, StringComparison.OrdinalIgnoreCase)));
        if (source.full == null)
            source = replicas.FirstOrDefault(r => r.exists && r.hash != null);
        if (source.full == null) return Task.FromResult<Stream?>(null);

        foreach (var r in replicas)
        {
            if (ReferenceEquals(r, source)) continue;
            if (!r.exists || (r.hash != null && source.hash != null && !r.hash.Equals(source.hash, StringComparison.OrdinalIgnoreCase)))
            {
                try
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(r.full)!);
                    CopyWithReplace(source.full, r.full);
                    var newHash = ComputeFileSha256(r.full);
                    var fi = new FileInfo(r.full);
                    idx.UpsertReplica(poolId, relativePath, r.d.RootPath, fi.Length, newHash, DateTime.UtcNow);
                    _logger.LogInformation("Self-healed {Path} on {Drive}", r.full, r.d.RootPath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed self-heal for {Path}", r.full);
                }
            }
        }

        try
        {
            var fi = new FileInfo(source.full);
            idx.UpsertReplica(poolId, relativePath, source.d.RootPath, fi.Length, source.hash ?? "", DateTime.UtcNow);
        }
        catch { }

        return Task.FromResult<Stream?>(File.OpenRead(source.full));
    }

    public Task<bool> DeleteFileAsync(Guid poolId, string relativePath)
    {
        if (!_pools.TryGetValue(poolId, out var pool)) return Task.FromResult(false);
        relativePath = SanitizeRelativePath(relativePath);
        var idx = new SqliteMetadataIndex(IndexPath);

        var deleted = false;
        foreach (var d in pool.Drives)
        {
            var full = Path.Combine(d.RootPath, relativePath);
            if (File.Exists(full))
            {
                try { File.Delete(full); deleted = true; UpdateDriveSpace(d); idx.RemoveReplica(poolId, relativePath, d.RootPath); }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete {Full}", full); }
            }
        }
        return Task.FromResult(deleted);
    }

    public Task<bool> FileExistsAsync(Guid poolId, string relativePath)
    {
        if (!_pools.TryGetValue(poolId, out var pool)) return Task.FromResult(false);
        relativePath = SanitizeRelativePath(relativePath);
        var exists = pool.Drives.Any(d => File.Exists(Path.Combine(d.RootPath, relativePath)));
        return Task.FromResult(exists);
    }

    public Task<List<string>> ListFilesAsync(Guid poolId, string pattern = "*")
    {
        if (!_pools.TryGetValue(poolId, out var pool)) return Task.FromResult(new List<string>());

        var results = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var d in pool.Drives)
        {
            if (!Directory.Exists(d.RootPath)) continue;
            try
            {
                foreach (var file in Directory.EnumerateFiles(d.RootPath, pattern, SearchOption.AllDirectories))
                {
                    var rel = Path.GetRelativePath(d.RootPath, file);
                    results.Add(rel);
                }
            }
            catch (Exception ex) { _logger.LogDebug(ex, "ListFiles skipped {Root}", d.RootPath); }
        }
        return Task.FromResult(results.ToList());
    }

    // ------- REBALANCE -------
    public async Task<bool> RebalancePoolAsync(Guid poolId)
    {
        if (!_pools.TryGetValue(poolId, out var pool))
        {
            _logger.LogWarning("Pool not found: {PoolId}", poolId);
            return false;
        }

        var idx = new SqliteMetadataIndex(IndexPath);

        var byDrive = new Dictionary<PoolDrive, HashSet<string>>();
        var allRel = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var d in pool.Drives)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (Directory.Exists(d.RootPath))
            {
                try
                {
                    foreach (var file in Directory.EnumerateFiles(d.RootPath, "*", SearchOption.AllDirectories))
                    {
                        var rel = Path.GetRelativePath(d.RootPath, file);
                        set.Add(rel);
                        allRel.Add(rel);
                    }
                }
                catch (Exception ex) { _logger.LogDebug(ex, "Rebalance list skip {Root}", d.RootPath); }
            }
            byDrive[d] = set;
        }

        foreach (var rel in allRel)
        {
            var rule = ResolveRule(pool, rel);
            var required = Math.Max(1, rule?.DuplicationLevel ?? 1);

            var holders = byDrive.Where(kv => kv.Value.Contains(rel)).Select(kv => kv.Key).ToList();

            string? referencePath = null;
            string? referenceHash = null;
            foreach (var d in holders)
            {
                var p = Path.Combine(d.RootPath, rel);
                if (File.Exists(p))
                {
                    referencePath ??= p;
                    referenceHash ??= ComputeFileSha256(p);
                }
            }
            if (referencePath is null || referenceHash is null) continue;

            if (holders.Count < required)
            {
                var size = new FileInfo(referencePath).Length;
                var candidates = SelectEligibleDrivesWeighted(pool, rel, size, rule)
                    .Where(d => !holders.Contains(d))
                    .Take(required - holders.Count)
                    .ToList();

                foreach (var d in candidates)
                {
                    var dest = Path.Combine(d.RootPath, rel);
                    Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
                    CopyWithReplace(referencePath, dest);
                    var h = ComputeFileSha256(dest);
                    if (!h.Equals(referenceHash, StringComparison.OrdinalIgnoreCase))
                        throw new IOException($"Rebalance hash mismatch for {rel} to {d.RootPath}");
                    byDrive[d].Add(rel);
                    UpdateDriveSpace(d);
                    idx.UpsertReplica(poolId, rel, d.RootPath, new FileInfo(dest).Length, h, DateTime.UtcNow);
                    _logger.LogInformation("Rebalance: replica {Rel} on {Drive}", rel, d.RootPath);
                }
            }

            if (holders.Count > required)
            {
                var ranked = RankDrivesForRuleWeighted(holders, rule).ToList();
                var keep = ranked.Take(required).ToHashSet();
                var remove = holders.Where(d => !keep.Contains(d)).ToList();
                foreach (var d in remove)
                {
                    var p = Path.Combine(d.RootPath, rel);
                    try { File.Delete(p); byDrive[d].Remove(rel); UpdateDriveSpace(d); idx.RemoveReplica(poolId, rel, d.RootPath); }
                    catch (Exception ex) { _logger.LogWarning(ex, "Failed removing extra replica {Rel} from {Drive}", rel, d.RootPath); }
                }
            }
        }

        _logger.LogInformation("Rebalance complete for pool {PoolId}", poolId);
        return true;
    }

    public Task<DriveHealth> CheckDriveHealthAsync(string drivePath)
    {
        try
        {
            if (!Directory.Exists(drivePath)) return Task.FromResult(DriveHealth.Warning);
            var test = Path.Combine(drivePath, $".healthcheck_{Guid.NewGuid():N}");
            File.WriteAllText(test, "ok");
            File.Delete(test);
            return Task.FromResult(DriveHealth.Healthy);
        }
        catch { return Task.FromResult(DriveHealth.Warning); }
    }

    public Task<bool> AddDriveToPoolAsync(Guid poolId, string drivePath)
    {
        if (!_pools.TryGetValue(poolId, out var pool)) return Task.FromResult(false);
        var d = CreatePoolDrive(drivePath);
        pool.Drives.Add(d);
        pool.UpdatedAt = DateTime.UtcNow;
        return Task.FromResult(true);
    }

    public Task<bool> RemoveDriveFromPoolAsync(Guid poolId, string drivePath)
    {
        if (!_pools.TryGetValue(poolId, out var pool)) return Task.FromResult(false);
        var removed = pool.Drives.RemoveAll(d =>
            string.Equals(d.RootPath, drivePath, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(d.DriveLetter, drivePath, StringComparison.OrdinalIgnoreCase)) > 0;
        if (removed) pool.UpdatedAt = DateTime.UtcNow;
        return Task.FromResult(removed);
    }

    public Task<StoragePool?> GetPoolAsync(Guid poolId)
        => Task.FromResult(_pools.TryGetValue(poolId, out var pool) ? pool : null);

    public Task<List<StoragePool>> GetAllPoolsAsync()
        => Task.FromResult(_pools.Values.ToList());

    // ---------- helpers ----------
    private static string SanitizeRelativePath(string path)
    {
        path = path.Replace('\\', Path.DirectorySeparatorChar)
                   .Replace('/', Path.DirectorySeparatorChar);
        while (path.StartsWith(Path.DirectorySeparatorChar)) path = path[1..];
        if (path.Contains(".." + Path.DirectorySeparatorChar)) throw new InvalidOperationException("Parent traversal not allowed.");
        return path;
    }

    private PoolDrive CreatePoolDrive(string drivePath)
    {
        var root = Path.GetPathRoot(drivePath) ?? drivePath;
        var label = "";
        long total = 0, free = 0;

        try
        {
            var di = new DriveInfo(root);
            label = SafeGet(() => di.VolumeLabel, "");
            total = SafeGet(() => di.TotalSize, 0L);
            free  = SafeGet(() => di.TotalFreeSpace, 0L);
        }
        catch { }

        var type = GuessDriveType(root);
        var tier = type switch { DriveType.NVMe or DriveType.SSD => DriveTier.Hot, DriveType.HDD => DriveTier.Warm, _ => DriveTier.Cold };
        var io = type switch { DriveType.NVMe => 3.0, DriveType.SSD => 2.0, DriveType.HDD => 1.0, DriveType.Network => 0.8, _ => 0.6 };

        return new PoolDrive
        {
            DriveLetter = root,
            RootPath = drivePath,
            Label = label,
            TotalSize = total,
            FreeSpace = free,
            Type = type,
            Health = DriveHealth.Healthy,
            LastHealthCheck = DateTime.UtcNow,
            Tier = tier,
            IoScore = io
        };
    }

    private static T SafeGet<T>(Func<T> f, T fallback) { try { return f(); } catch { return fallback; } }

    private static DriveType GuessDriveType(string root)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            if (root.StartsWith("\\\\") || root.StartsWith("//")) return DriveType.Network;
            return DriveType.HDD;
        }
        return DriveType.HDD;
    }

    private IEnumerable<PoolDrive> SelectEligibleDrivesWeighted(StoragePool pool, string relativePath, long fileSize, PoolRule? rule)
    {
        var candidates = pool.Drives
            .Where(d => d.Health == DriveHealth.Healthy)
            .Where(d => d.FreeSpace == 0 || d.FreeSpace > Math.Max(fileSize, 0))
            .ToList();

        if (!string.IsNullOrWhiteSpace(rule?.TargetDrive))
        {
            var match = candidates.Where(d =>
                string.Equals(d.RootPath, rule.TargetDrive, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(d.DriveLetter, rule.TargetDrive, StringComparison.OrdinalIgnoreCase)).ToList();
            if (match.Any()) candidates = match;
        }

        if (rule?.MaxFileSize is long lim && lim > 0)
            candidates = candidates.Where(_ => fileSize <= lim).ToList();

        var preferredTier = rule?.PreferredTier;
        IEnumerable<(PoolDrive d, double score)> scored = candidates.Select(d =>
        {
            var spaceRatio = d.TotalSize > 0 ? (double)d.FreeSpace / d.TotalSize : 0.5;
            var tierBoost = (preferredTier == null) ? 1.0 :
                (d.Tier == preferredTier ? 1.2 : (preferredTier == DriveTier.Hot && d.Tier == DriveTier.Warm ? 1.0 : 0.8));
            var ssdBoost = (rule?.PreferSSD == true && (d.Type == DriveType.SSD || d.Type == DriveType.NVMe)) ? 1.1 : 1.0;
            var healthW = d.Health switch { DriveHealth.Healthy => 1.0, DriveHealth.Warning => 0.6, DriveHealth.Critical => 0.2, _ => 0.5 };
            var score = (spaceRatio * 0.45) + (d.IoScore/3.0 * 0.35) + (healthW * 0.10);
            score *= tierBoost * ssdBoost;
            return (d, score);
        });

        var ordered = scored.OrderByDescending(x => x.score).Select(x => x.d).ToList();
        return ordered;
    }

    private static IEnumerable<PoolDrive> RankDrivesForRuleWeighted(IEnumerable<PoolDrive> drives, PoolRule? rule)
    {
        var pool = new StoragePool { Drives = drives.ToList() };
        return new FilePoolService(new Microsoft.Extensions.Logging.Abstractions.NullLogger<FilePoolService>())
            .SelectEligibleDrivesWeighted(pool, "", 0, rule);
    }

    private static bool GlobMatch(string path, string pattern)
    {
        char ds = Path.DirectorySeparatorChar;

        string norm(string s) => s.Replace('\\', ds).Replace('/', ds);
        path = norm(path);
        pattern = norm(pattern);

        if (string.Equals(path, pattern, StringComparison.OrdinalIgnoreCase)) return true;

        var parts = pattern.Split(ds);
        int pi = 0;
        int si = 0;
        var segs = path.Split(ds);

        while (pi < parts.Length && si <= segs.Length)
        {
            if (pi < parts.Length && parts[pi] == "**")
            {
                if (pi == parts.Length - 1) return true;
                pi++;
                while (si < segs.Length)
                {
                    if (SegMatch(segs[si], parts[pi])) { si++; pi++; break; }
                    si++;
                }
                continue;
            }
            if (pi < parts.Length && si < segs.Length && SegMatch(segs[si], parts[pi])) { pi++; si++; continue; }
            return false;
        }
        return pi == parts.Length && si == segs.Length;

        static bool SegMatch(string seg, string pat)
        {
            if (pat == "*") return true;
            if (!pat.Contains('*')) return string.Equals(seg, pat, StringComparison.OrdinalIgnoreCase);
            var tokens = pat.Split('*');
            int pos = 0;
            foreach (var t in tokens)
            {
                if (t.Length == 0) continue;
                var idx = seg.IndexOf(t, pos, StringComparison.OrdinalIgnoreCase);
                if (idx < 0) return false;
                pos = idx + t.Length;
            }
            return (pat.EndsWith('*') || pos == seg.Length);
        }
    }

    private static PoolRule? ResolveRule(StoragePool pool, string relativePath)
    {
        foreach (var r in pool.Rules)
        {
            if (GlobMatch(relativePath, r.Pattern)) return r;
        }
        return null;
    }

    private static void AtomicReplace(string tempPath, string finalPath)
    {
        if (File.Exists(finalPath))
        {
            try
            {
                var bak = finalPath + ".bak_" + Guid.NewGuid().ToString("N");
                File.Replace(tempPath, finalPath, bak, ignoreMetadataErrors: true);
                TryDelete(bak);
                return;
            }
            catch { }
            TryDelete(finalPath);
        }
        File.Move(tempPath, finalPath);
    }

    private static void CopyWithReplace(string srcPath, string destPath)
    {
        var temp = destPath + "." + Guid.NewGuid().ToString("N") + ".tmp";
        using (var s = File.OpenRead(srcPath))
        using (var d = File.Create(temp))
            s.CopyTo(d);

        if (File.Exists(destPath)) TryDelete(destPath);
        File.Move(temp, destPath);
    }

    private static string ComputeFileSha256(string path)
    {
        using var sha = System.Security.Cryptography.SHA256.Create();
        using var fs = File.OpenRead(path);
        var hash = sha.ComputeHash(fs);
        return Convert.ToHexString(hash);
    }

    private void UpdateDriveSpace(PoolDrive drive)
    {
        try
        {
            var root = string.IsNullOrWhiteSpace(drive.DriveLetter) ? drive.RootPath : drive.DriveLetter;
            var di = new DriveInfo(Path.GetPathRoot(root) ?? root);
            drive.FreeSpace = di.TotalFreeSpace;
            drive.TotalSize = di.TotalSize;
            drive.LastHealthCheck = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Free space refresh failed for {Root}", drive.RootPath);
        }
    }

    private static void TryDelete(string p){ try { if (File.Exists(p)) File.Delete(p); } catch {} }
}
