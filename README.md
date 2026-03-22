# ForgeDB

A production-grade relational database engine written from scratch in Rust. **4.2 MB** single binary with MySQL and SQL Server (TDS) wire protocol support.

**Beats PostgreSQL** on aggregate queries (2x faster) and concurrent reads (1.5x faster). Within 1.2-3x on UPDATE, transactions, JOINs, and full scans.

## Features

### SQL Engine
- Full DML: INSERT, SELECT, UPDATE, DELETE with complex WHERE clauses
- JOINs: INNER, LEFT, RIGHT, FULL OUTER, CROSS, self-join
- Aggregates: COUNT, SUM, AVG, MIN, MAX with GROUP BY / HAVING
- Subqueries: IN, NOT IN, EXISTS, NOT EXISTS, scalar subqueries
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE
- CTEs (WITH ... AS), UNION / UNION ALL, DISTINCT, ORDER BY, LIMIT + OFFSET
- CASE expressions, CAST, COALESCE, LIKE, BETWEEN, IN list
- INSERT ... SELECT, TRUNCATE TABLE, INSERT ON DUPLICATE KEY UPDATE
- String functions: UPPER, LOWER, LENGTH, SUBSTRING, REPLACE, TRIM, CONCAT, REVERSE, LPAD, RPAD
- Math functions: ABS, ROUND, CEIL, FLOOR, SQRT, POWER, MOD, SIGN
- Full-text search: CONTAINS(), FREETEXT()

### Data Types
- INT, BIGINT, FLOAT, DECIMAL(p,s), BOOLEAN
- VARCHAR(n), TEXT, JSON, UUID
- DATE, TIME, DATETIME
- VARBINARY(n)

### ACID Transactions
- MVCC snapshot isolation with version headers (xmin/xmax)
- Write-Ahead Log (WAL) with crash recovery (redo-only)
- Undo log for ROLLBACK with physical tuple restoration
- BEGIN / COMMIT / ROLLBACK with session-level transactions
- Savepoints (SAVEPOINT / ROLLBACK TO / RELEASE)
- Row-level lock manager with deadlock detection (wait-for graph cycle detection)
- Configurable lock timeout (SET LOCK_TIMEOUT)

### Constraints & Integrity
- PRIMARY KEY (single and composite)
- UNIQUE constraints
- CHECK constraints (enforced on INSERT and UPDATE)
- FOREIGN KEY with ON DELETE CASCADE / SET NULL / RESTRICT
- NOT NULL with DEFAULT values
- AUTO_INCREMENT / IDENTITY

### Storage Engine
- 16KB page-based heap file storage with slotted pages
- Concurrent buffer pool with per-page RwLocks
- B-tree secondary indexes (single and composite)
- Clustered B+ tree indexes on primary keys
- Overflow pages for large tuples (TOAST-style)
- Parallel sequential scan across CPU cores
- Direct page scan bypassing local cache for read-only queries
- COUNT(*) fast path: slot-counting without tuple deserialization

### Query Optimizer
- Cost-based optimizer with table/column statistics (ANALYZE TABLE)
- Index scan selection via selectivity estimation
- Index-only scans when covering index available
- Automatic hash join selection for large tables
- Grace hash join with disk-based partitioning for huge datasets
- LIMIT pushdown into sequential scans
- Query plan cache (RwLock-based, bounded at 10K entries)
- EXPLAIN plan output

### Wire Protocols
- **MySQL protocol** — compatible with mysql CLI, DBeaver, WordPress
- **TDS protocol (SQL Server)** — compatible with Microsoft.Data.SqlClient, .NET applications
  - Binary encoding: SQLINT4/INT8 for integers, SQLFLT8 for floats, SQLBIT for booleans
  - NVARCHAR for strings with proper collation
  - Full PRELOGIN/LOGIN7 handshake, FEATUREEXTACK, sp_reset_connection
  - T-SQL rewriting: IDENTITY→AUTO_INCREMENT, TOP→LIMIT, ISNULL→COALESCE, [brackets], N'strings'

### Operational Features
- Stored procedures (CREATE PROCEDURE / EXEC)
- Triggers (AFTER INSERT/UPDATE/DELETE)
- User management (CREATE USER / GRANT / REVOKE)
- Sequences (CREATE SEQUENCE / NEXTVAL)
- Prepared statements (PREPARE / EXECUTE)
- Cursors (DECLARE / OPEN / FETCH NEXT / CLOSE / DEALLOCATE)
- Backup / Restore (BACKUP DATABASE TO / RESTORE DATABASE FROM)
- Partitioned tables (PARTITION BY RANGE)
- Temp tables with connection-scoped lifecycle
- Multiple database support (CREATE DATABASE / USE)
- Query plan caching with DDL invalidation

## Quick Start

```bash
# Build
cargo build --release

# Start server (MySQL on 3307, TDS on 1433)
./target/release/forgedb-server ./data

# Connect via SQL Server client
sqlcmd -S 127.0.0.1,1433 -U sa -P x -C

# Connect via MySQL client
mysql -h 127.0.0.1 -P 3307
```

## .NET Integration

```csharp
using Microsoft.Data.SqlClient;

var conn = new SqlConnection("Server=127.0.0.1,1433;User Id=sa;Password=x;Encrypt=false;TrustServerCertificate=True;");
conn.Open();

new SqlCommand("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))", conn).ExecuteNonQuery();
new SqlCommand("INSERT INTO users VALUES (1, 'Alice')", conn).ExecuteNonQuery();

using var reader = new SqlCommand("SELECT * FROM users", conn).ExecuteReader();
while (reader.Read())
    Console.WriteLine($"{reader.GetInt32(0)}: {reader.GetValue(1)}");
```

## Benchmarks vs PostgreSQL 15

| Benchmark | ForgeDB | PostgreSQL | Result |
|-----------|---------|------------|--------|
| Aggregate queries | 0.006s | 0.012s | **ForgeDB 2x faster** |
| Concurrent reads (20 clients) | 0.022s | 0.033s | **ForgeDB 1.5x faster** |
| Point SELECT by PK | 0.224s | 0.186s | PG 1.2x faster |
| UPDATE 500 rows | 0.061s | 0.043s | PG 1.4x faster |
| Transaction throughput | 0.182s | 0.114s | PG 1.6x faster |
| INNER JOIN | 0.003s | 0.001s | PG 2.6x faster |
| Full table scan | 0.002s | 0.001s | PG 2.8x faster |

*5000 rows, 20 concurrent clients, single-connection queries, release build.*

## Testing

```bash
# 551 Rust tests
cargo test

# 189 .NET integration tests (186 pass, 3 skipped)
cd dotnet-tests/ForgeDB.AcidTests
dotnet test

# Benchmarks vs PostgreSQL
cd dotnet-tests/ForgeDB.Benchmarks
dotnet run -c Release
```

**737 total tests** covering ACID compliance, concurrency (100 concurrent clients), foreign keys, constraints, window functions, subqueries, memory leak detection, and all SQL features.

## Architecture

```
┌─────────────────────────────────────────────┐
│  Wire Protocols (MySQL + TDS)               │
├─────────────────────────────────────────────┤
│  SQL Parser (sqlparser crate + fast path)   │
├─────────────────────────────────────────────┤
│  Query Planner (cost-based optimizer)       │
├─────────────────────────────────────────────┤
│  Executor (volcano-style + streaming agg)   │
├─────────────────────────────────────────────┤
│  Transaction Manager (MVCC + WAL + Undo)    │
├─────────────────────────────────────────────┤
│  Lock Manager (row-level + deadlock detect) │
├─────────────────────────────────────────────┤
│  Buffer Pool (concurrent, per-page RwLock)  │
├─────────────────────────────────────────────┤
│  Storage (heap files + B-tree indexes)      │
├─────────────────────────────────────────────┤
│  Disk Manager (16KB pages, overflow/TOAST)  │
└─────────────────────────────────────────────┘
```

## Binary Size

```
$ ls -lh target/release/forgedb-server
4.2M  forgedb-server
```

Full RDBMS with dual wire protocols in a 4.2 MB statically-linked binary.

## License

MIT
