<?php
/**
 * ForgeDB vs MySQL Benchmark Suite
 *
 * Tests WordPress-like database operations against both ForgeDB and MySQL.
 *
 * Usage:
 *   php benchmark.php forgedb   # Test ForgeDB on port 3307
 *   php benchmark.php mysql     # Test MySQL on port 3306
 *   php benchmark.php both      # Compare both (default)
 *
 * Requirements:
 *   - PHP 8.0+ with mysqli extension
 *   - ForgeDB running: cargo run --release --bin forgedb-server -- ./bench_data 0.0.0.0:3307
 *   - MySQL running on port 3306 (optional, for comparison)
 */

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

$FORGEDB_HOST = '127.0.0.1';
$FORGEDB_PORT = 3307;
$FORGEDB_USER = 'root';
$FORGEDB_PASS = '';

$MYSQL_HOST = '127.0.0.1';
$MYSQL_PORT = 3306;
$MYSQL_USER = 'root';
$MYSQL_PASS = '';
$MYSQL_DB   = 'forgedb_bench';

$mode = $argv[1] ?? 'both';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

class BenchResult {
    public string $name;
    public float $duration_ms;
    public bool $success;
    public string $error;
    public int $rows_affected;

    public function __construct(string $name, float $duration_ms, bool $success, string $error = '', int $rows = 0) {
        $this->name = $name;
        $this->duration_ms = $duration_ms;
        $this->success = $success;
        $this->error = $error;
        $this->rows_affected = $rows;
    }
}

function run_benchmark(mysqli $conn, string $name, callable $fn): BenchResult {
    $start = hrtime(true);
    try {
        $rows = $fn($conn);
        $elapsed = (hrtime(true) - $start) / 1e6; // nanoseconds to ms
        return new BenchResult($name, $elapsed, true, '', $rows ?? 0);
    } catch (\Throwable $e) {
        $elapsed = (hrtime(true) - $start) / 1e6;
        return new BenchResult($name, $elapsed, false, $e->getMessage());
    }
}

function query(mysqli $conn, string $sql): mysqli_result|bool {
    $result = $conn->query($sql);
    if ($result === false) {
        throw new \RuntimeException("Query failed: " . $conn->error . " | SQL: " . substr($sql, 0, 200));
    }
    return $result;
}

// ---------------------------------------------------------------------------
// Benchmark Definitions
// ---------------------------------------------------------------------------

function benchmark_suite(mysqli $conn, string $engine_name): array {
    $results = [];

    // -----------------------------------------------------------------------
    // 1. Schema Creation (WordPress-like tables)
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "CREATE TABLE wp_options", function($c) {
        query($c, "DROP TABLE IF EXISTS wp_options");
        query($c, "
            CREATE TABLE wp_options (
                option_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                option_name VARCHAR(191) NOT NULL,
                option_value LONGTEXT NOT NULL,
                autoload VARCHAR(20) NOT NULL
            )
        ");
        return 0;
    });

    $results[] = run_benchmark($conn, "CREATE TABLE wp_posts", function($c) {
        query($c, "DROP TABLE IF EXISTS wp_posts");
        query($c, "
            CREATE TABLE wp_posts (
                ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                post_author BIGINT NOT NULL,
                post_date DATETIME NOT NULL,
                post_content LONGTEXT NOT NULL,
                post_title VARCHAR(255) NOT NULL,
                post_status VARCHAR(20) NOT NULL,
                post_type VARCHAR(20) NOT NULL
            )
        ");
        return 0;
    });

    $results[] = run_benchmark($conn, "CREATE TABLE wp_postmeta", function($c) {
        query($c, "DROP TABLE IF EXISTS wp_postmeta");
        query($c, "
            CREATE TABLE wp_postmeta (
                meta_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                post_id BIGINT NOT NULL,
                meta_key VARCHAR(255),
                meta_value LONGTEXT
            )
        ");
        return 0;
    });

    $results[] = run_benchmark($conn, "CREATE TABLE wp_users", function($c) {
        query($c, "DROP TABLE IF EXISTS wp_users");
        query($c, "
            CREATE TABLE wp_users (
                ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                user_login VARCHAR(60) NOT NULL,
                user_pass VARCHAR(255) NOT NULL,
                user_email VARCHAR(100) NOT NULL,
                user_registered DATETIME NOT NULL,
                display_name VARCHAR(250) NOT NULL
            )
        ");
        return 0;
    });

    // -----------------------------------------------------------------------
    // 2. Bulk INSERT (wp_options - WordPress settings)
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "INSERT 100 wp_options", function($c) {
        $count = 0;
        for ($i = 1; $i <= 100; $i++) {
            query($c, "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('option_$i', 'value_$i', 'yes')");
            $count++;
        }
        return $count;
    });

    // -----------------------------------------------------------------------
    // 3. Bulk INSERT (wp_users)
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "INSERT 50 wp_users", function($c) {
        $count = 0;
        for ($i = 1; $i <= 50; $i++) {
            query($c, "INSERT INTO wp_users (user_login, user_pass, user_email, user_registered, display_name) VALUES ('user$i', 'hashed_pass_$i', 'user$i@example.com', '2024-01-01 00:00:00', 'User $i')");
            $count++;
        }
        return $count;
    });

    // -----------------------------------------------------------------------
    // 4. Bulk INSERT (wp_posts - blog posts)
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "INSERT 200 wp_posts", function($c) {
        $count = 0;
        $statuses = ['publish', 'draft', 'private', 'pending'];
        $types = ['post', 'page', 'attachment'];
        for ($i = 1; $i <= 200; $i++) {
            $author = ($i % 50) + 1;
            $status = $statuses[$i % 4];
            $type = $types[$i % 3];
            $content = str_repeat("This is the content of post $i. ", 10);
            $title = "Post Title Number $i";
            $content_escaped = addslashes($content);
            query($c, "INSERT INTO wp_posts (post_author, post_date, post_content, post_title, post_status, post_type) VALUES ($author, '2024-06-15 12:00:00', '$content_escaped', '$title', '$status', '$type')");
            $count++;
        }
        return $count;
    });

    // -----------------------------------------------------------------------
    // 5. Bulk INSERT (wp_postmeta)
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "INSERT 500 wp_postmeta", function($c) {
        $count = 0;
        for ($i = 1; $i <= 500; $i++) {
            $post_id = ($i % 200) + 1;
            $key = "meta_key_" . ($i % 20);
            $value = "meta_value_$i";
            query($c, "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES ($post_id, '$key', '$value')");
            $count++;
        }
        return $count;
    });

    // -----------------------------------------------------------------------
    // 6. SELECT - Point lookups (WordPress option reads)
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT 100 wp_options by name", function($c) {
        $count = 0;
        for ($i = 1; $i <= 100; $i++) {
            $r = query($c, "SELECT option_value FROM wp_options WHERE option_name = 'option_$i'");
            if ($r instanceof mysqli_result) {
                $count += $r->num_rows;
                $r->free();
            }
        }
        return $count;
    });

    // -----------------------------------------------------------------------
    // 7. SELECT - Full table scan
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT * FROM wp_posts (full scan)", function($c) {
        $r = query($c, "SELECT * FROM wp_posts");
        $count = $r->num_rows;
        $r->free();
        return $count;
    });

    // -----------------------------------------------------------------------
    // 8. SELECT - Filtered scan
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT wp_posts WHERE status=publish", function($c) {
        $r = query($c, "SELECT * FROM wp_posts WHERE post_status = 'publish'");
        $count = $r->num_rows;
        $r->free();
        return $count;
    });

    // -----------------------------------------------------------------------
    // 9. SELECT - LIKE search
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT wp_posts WHERE title LIKE", function($c) {
        $r = query($c, "SELECT * FROM wp_posts WHERE post_title LIKE '%Number 1%'");
        $count = $r->num_rows;
        $r->free();
        return $count;
    });

    // -----------------------------------------------------------------------
    // 10. SELECT - COUNT aggregate
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT COUNT(*) FROM wp_posts", function($c) {
        $r = query($c, "SELECT COUNT(*) FROM wp_posts");
        $row = $r->fetch_row();
        $r->free();
        return (int)$row[0];
    });

    // -----------------------------------------------------------------------
    // 11. SELECT - JOIN (posts + users)
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT posts JOIN users", function($c) {
        $r = query($c, "SELECT wp_posts.post_title, wp_users.display_name FROM wp_posts INNER JOIN wp_users ON wp_posts.post_author = wp_users.ID");
        $count = $r->num_rows;
        $r->free();
        return $count;
    });

    // -----------------------------------------------------------------------
    // 12. SELECT - ORDER BY + LIMIT
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT posts ORDER BY LIMIT 10", function($c) {
        $r = query($c, "SELECT * FROM wp_posts ORDER BY ID DESC LIMIT 10");
        $count = $r->num_rows;
        $r->free();
        return $count;
    });

    // -----------------------------------------------------------------------
    // 13. UPDATE - Single row
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "UPDATE 50 wp_options", function($c) {
        $count = 0;
        for ($i = 1; $i <= 50; $i++) {
            query($c, "UPDATE wp_options SET option_value = 'updated_$i' WHERE option_name = 'option_$i'");
            $count++;
        }
        return $count;
    });

    // -----------------------------------------------------------------------
    // 14. UPDATE - Bulk update
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "UPDATE wp_posts SET status=draft", function($c) {
        query($c, "UPDATE wp_posts SET post_status = 'draft' WHERE post_status = 'pending'");
        return $c->affected_rows;
    });

    // -----------------------------------------------------------------------
    // 15. DELETE - Selective
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "DELETE wp_postmeta subset", function($c) {
        query($c, "DELETE FROM wp_postmeta WHERE meta_key = 'meta_key_0'");
        return $c->affected_rows;
    });

    // -----------------------------------------------------------------------
    // 16. SELECT after mutations
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT COUNT(*) after mutations", function($c) {
        $r = query($c, "SELECT COUNT(*) FROM wp_postmeta");
        $row = $r->fetch_row();
        $r->free();
        return (int)$row[0];
    });

    // -----------------------------------------------------------------------
    // 17. SHOW TABLES
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SHOW TABLES", function($c) {
        $r = query($c, "SHOW TABLES");
        $count = $r->num_rows;
        $r->free();
        return $count;
    });

    // -----------------------------------------------------------------------
    // 18. IN query
    // -----------------------------------------------------------------------
    $results[] = run_benchmark($conn, "SELECT wp_posts WHERE ID IN (...)", function($c) {
        $r = query($c, "SELECT * FROM wp_posts WHERE ID IN (1, 5, 10, 50, 100, 150, 200)");
        $count = $r->num_rows;
        $r->free();
        return $count;
    });

    return $results;
}

// ---------------------------------------------------------------------------
// Connection + Execution
// ---------------------------------------------------------------------------

function connect_db(string $host, int $port, string $user, string $pass, string $db = ''): ?mysqli {
    mysqli_report(MYSQLI_REPORT_OFF);
    $conn = @new mysqli($host, $user, $pass, $db, $port);
    if ($conn->connect_error) {
        return null;
    }
    return $conn;
}

function print_results(string $engine, array $results): void {
    $total_ms = 0;
    $passed = 0;
    $failed = 0;

    echo "\n";
    echo str_repeat("=", 80) . "\n";
    echo "  $engine Benchmark Results\n";
    echo str_repeat("=", 80) . "\n";
    echo sprintf("  %-45s %10s %8s %s\n", "Test", "Time (ms)", "Rows", "Status");
    echo str_repeat("-", 80) . "\n";

    foreach ($results as $r) {
        $status = $r->success ? "  OK" : "FAIL";
        $time_str = sprintf("%10.2f", $r->duration_ms);
        $rows_str = sprintf("%8d", $r->rows_affected);
        $line = sprintf("  %-45s %s %s %s", $r->name, $time_str, $rows_str, $status);

        if (!$r->success) {
            $line .= " | " . substr($r->error, 0, 60);
            $failed++;
        } else {
            $passed++;
        }

        echo $line . "\n";
        $total_ms += $r->duration_ms;
    }

    echo str_repeat("-", 80) . "\n";
    echo sprintf("  %-45s %10.2f %8s\n", "TOTAL", $total_ms, "");
    echo sprintf("  Passed: %d  |  Failed: %d  |  Total: %d\n", $passed, $failed, count($results));
    echo str_repeat("=", 80) . "\n";
}

function print_comparison(array $forge_results, array $mysql_results): void {
    echo "\n";
    echo str_repeat("=", 90) . "\n";
    echo "  Comparison: ForgeDB vs MySQL\n";
    echo str_repeat("=", 90) . "\n";
    echo sprintf("  %-40s %12s %12s %10s\n", "Test", "ForgeDB(ms)", "MySQL(ms)", "Ratio");
    echo str_repeat("-", 90) . "\n";

    $forge_total = 0;
    $mysql_total = 0;

    for ($i = 0; $i < count($forge_results); $i++) {
        $f = $forge_results[$i];
        $m = $mysql_results[$i] ?? null;

        if (!$f->success || !$m || !$m->success) {
            $ratio = "N/A";
        } else {
            $r = $m->duration_ms > 0 ? $f->duration_ms / $m->duration_ms : 0;
            $ratio = sprintf("%.2fx", $r);
        }

        $ft = sprintf("%12.2f", $f->duration_ms);
        $mt = $m ? sprintf("%12.2f", $m->duration_ms) : "       N/A";

        echo sprintf("  %-40s %s %s %10s\n", $f->name, $ft, $mt, $ratio);

        $forge_total += $f->duration_ms;
        if ($m) $mysql_total += $m->duration_ms;
    }

    echo str_repeat("-", 90) . "\n";
    $total_ratio = $mysql_total > 0 ? sprintf("%.2fx", $forge_total / $mysql_total) : "N/A";
    echo sprintf("  %-40s %12.2f %12.2f %10s\n", "TOTAL", $forge_total, $mysql_total, $total_ratio);
    echo str_repeat("=", 90) . "\n";
    echo "\n  Ratio < 1.0x = ForgeDB is faster | Ratio > 1.0x = MySQL is faster\n\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

echo "\n  ForgeDB vs MySQL Benchmark Suite\n";
echo "  ================================\n\n";

$forge_results = null;
$mysql_results = null;

if ($mode === 'forgedb' || $mode === 'both') {
    echo "  Connecting to ForgeDB ($FORGEDB_HOST:$FORGEDB_PORT)...\n";
    $forge_conn = connect_db($FORGEDB_HOST, $FORGEDB_PORT, $FORGEDB_USER, $FORGEDB_PASS);
    if ($forge_conn) {
        echo "  Connected! Running benchmarks...\n";
        $forge_results = benchmark_suite($forge_conn, 'ForgeDB');
        print_results('ForgeDB', $forge_results);
        $forge_conn->close();
    } else {
        echo "  SKIP: Cannot connect to ForgeDB on port $FORGEDB_PORT\n";
        echo "  Start it with: cargo run --release --bin forgedb-server -- ./bench_data 0.0.0.0:3307\n";
    }
}

if ($mode === 'mysql' || $mode === 'both') {
    echo "\n  Connecting to MySQL ($MYSQL_HOST:$MYSQL_PORT)...\n";
    $mysql_conn = connect_db($MYSQL_HOST, $MYSQL_PORT, $MYSQL_USER, $MYSQL_PASS);
    if ($mysql_conn) {
        echo "  Connected! Setting up database...\n";
        $mysql_conn->query("CREATE DATABASE IF NOT EXISTS $MYSQL_DB");
        $mysql_conn->select_db($MYSQL_DB);
        echo "  Running benchmarks...\n";
        $mysql_results = benchmark_suite($mysql_conn, 'MySQL');
        print_results('MySQL', $mysql_results);
        $mysql_conn->close();
    } else {
        echo "  SKIP: Cannot connect to MySQL on port $MYSQL_PORT\n";
    }
}

if ($forge_results && $mysql_results) {
    print_comparison($forge_results, $mysql_results);
}

echo "  Done.\n\n";
