using System;
using System.Collections.Generic;
using System.IO;
using System.Data;
using Microsoft.Data.Sqlite;

namespace KydrasStorage.Core.Services;

public class SqliteMetadataIndex
{
    private readonly string _dbPath;

    public SqliteMetadataIndex(string dbPath)
    {
        _dbPath = dbPath;
        Directory.CreateDirectory(Path.GetDirectoryName(_dbPath)!);
        EnsureSchema();
    }

    private SqliteConnection Open()
    {
        var conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadWriteCreate;Cache=Shared");
        conn.Open();
        return conn;
    }

    private void EnsureSchema()
    {
        using var c = Open();
        var cmd = c.CreateCommand();
        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS replicas(
  pool_id TEXT NOT NULL,
  rel_path TEXT NOT NULL,
  drive_root TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  modified_utc TEXT NOT NULL,
  PRIMARY KEY(pool_id, rel_path, drive_root)
);
CREATE INDEX IF NOT EXISTS idx_replicas_lookup ON replicas(pool_id, rel_path);
";
        cmd.ExecuteNonQuery();
    }

    public void UpsertReplica(Guid poolId, string relPath, string driveRoot, long size, string sha256, DateTime modifiedUtc)
    {
        using var c = Open();
        var cmd = c.CreateCommand();
        cmd.CommandText = @"
INSERT INTO replicas(pool_id, rel_path, drive_root, size_bytes, sha256, modified_utc)
VALUES($p,$r,$d,$s,$h,$m)
ON CONFLICT(pool_id, rel_path, drive_root) DO UPDATE SET
  size_bytes=excluded.size_bytes, sha256=excluded.sha256, modified_utc=excluded.modified_utc;";
        cmd.Parameters.AddWithValue("$p", poolId.ToString("D"));
        cmd.Parameters.AddWithValue("$r", relPath);
        cmd.Parameters.AddWithValue("$d", driveRoot);
        cmd.Parameters.AddWithValue("$s", size);
        cmd.Parameters.AddWithValue("$h", sha256);
        cmd.Parameters.AddWithValue("$m", modifiedUtc.ToUniversalTime().ToString("o"));
        cmd.ExecuteNonQuery();
    }

    public void RemoveReplica(Guid poolId, string relPath, string driveRoot)
    {
        using var c = Open();
        var cmd = c.CreateCommand();
        cmd.CommandText = "DELETE FROM replicas WHERE pool_id=$p AND rel_path=$r AND drive_root=$d";
        cmd.Parameters.AddWithValue("$p", poolId.ToString("D"));
        cmd.Parameters.AddWithValue("$r", relPath);
        cmd.Parameters.AddWithValue("$d", driveRoot);
        cmd.ExecuteNonQuery();
    }

    public List<(string driveRoot, long size, string sha256, DateTime modifiedUtc)> GetReplicas(Guid poolId, string relPath)
    {
        var list = new List<(string,long,string,DateTime)>();
        using var c = Open();
        var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT drive_root,size_bytes,sha256,modified_utc FROM replicas WHERE pool_id=$p AND rel_path=$r";
        cmd.Parameters.AddWithValue("$p", poolId.ToString("D"));
        cmd.Parameters.AddWithValue("$r", relPath);
        using var rd = cmd.ExecuteReader();
        while (rd.Read())
        {
            var d = rd.GetString(0);
            var s = rd.GetInt64(1);
            var h = rd.GetString(2);
            var m = DateTime.Parse(rd.GetString(3), null, System.Globalization.DateTimeStyles.RoundtripKind);
            list.Add((d,s,h,m));
        }
        return list;
    }
}
