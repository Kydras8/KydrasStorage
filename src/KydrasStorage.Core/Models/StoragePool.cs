using System;
using System.Collections.Generic;
using System.Linq;

namespace KydrasStorage.Core.Models;

public class StoragePool
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = string.Empty;
    public string MountPoint { get; set; } = string.Empty;
    public PoolType Type { get; set; }
    public List<PoolDrive> Drives { get; set; } = new();
    public List<PoolRule> Rules { get; set; } = new();
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;

    public long TotalCapacity => Drives.Sum(d => d.TotalSize);
    public long AvailableCapacity => Drives.Sum(d => d.FreeSpace);
    public double Utilization => TotalCapacity > 0 ? (TotalCapacity - AvailableCapacity) / (double)TotalCapacity : 0;
}

public class PoolDrive
{
    public string DriveLetter { get; set; } = string.Empty; // Root (e.g., "C:\", "/")
    public string RootPath { get; set; } = string.Empty;    // Real root (e.g., "C:\PoolA", "/mnt/d1")
    public string Label { get; set; } = string.Empty;
    public long TotalSize { get; set; }
    public long FreeSpace { get; set; }
    public DriveType Type { get; set; }
    public DriveHealth Health { get; set; } = DriveHealth.Unknown;
    public DateTime LastHealthCheck { get; set; }

    // NEW: placement hints
    public DriveTier Tier { get; set; } = DriveTier.Warm;
    public double IoScore { get; set; } = 1.0; // relative IO performance (1=baseline, 2=fast, etc.)
}

public class PoolRule
{
    public string Pattern { get; set; } = string.Empty; // "*.mp4" or "**/videos/*"
    public string TargetDrive { get; set; } = string.Empty; // Specific drive or "any"
    public int DuplicationLevel { get; set; } = 1; // 1=no duplication, 2=mirror
    public bool PreferSSD { get; set; }
    public long? MaxFileSize { get; set; } // Optional limit (bytes)

    // Optional tiering (hot/warm/cold). If null, use weighted default.
    public DriveTier? PreferredTier { get; set; } = null;
}

public enum PoolType { JBOD, Mirror, Performance, Archive, Custom }
public enum DriveType { HDD, SSD, NVMe, Network, Removable }
public enum DriveHealth { Unknown, Healthy, Warning, Critical, Failed }
public enum DriveTier { Hot, Warm, Cold }
