<?php
/**
 * WordPress SQL Compatibility Test Suite
 *
 * Contains the EXACT SQL statements that WordPress core sends to MySQL.
 * Extracted from:
 *   - wp-admin/includes/schema.php          (CREATE TABLE DDL)
 *   - wp-includes/class-wpdb.php            (connection init, query dispatch)
 *   - wp-admin/includes/upgrade.php          (install + upgrade DML)
 *   - wp-includes/option.php                 (options CRUD)
 *   - wp-includes/meta.php                   (meta CRUD)
 *   - wp-includes/post.php                   (post operations)
 *   - wp-includes/comment.php                (comment operations)
 *   - wp-includes/taxonomy.php               (term/taxonomy operations)
 *   - wp-includes/user.php                   (user operations)
 *   - wp-includes/class-wp-query.php         (WP_Query SELECT patterns)
 *
 * Usage:
 *   php wordpress_queries.php forgedb   # Test ForgeDB on port 3307
 *   php wordpress_queries.php mysql     # Test MySQL on port 3306
 *   php wordpress_queries.php both      # Compare both (default)
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
$MYSQL_DB   = 'wordpress_test';

$mode = $argv[1] ?? 'both';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

class TestResult {
    public string $name;
    public string $category;
    public float $duration_ms;
    public bool $success;
    public string $error;
    public int $rows_affected;

    public function __construct(string $name, string $category, float $duration_ms, bool $success, string $error = '', int $rows = 0) {
        $this->name = $name;
        $this->category = $category;
        $this->duration_ms = $duration_ms;
        $this->success = $success;
        $this->error = $error;
        $this->rows_affected = $rows;
    }
}

function run_test(mysqli $conn, string $name, string $category, callable $fn): TestResult {
    $start = hrtime(true);
    try {
        $rows = $fn($conn);
        $elapsed = (hrtime(true) - $start) / 1e6;
        return new TestResult($name, $category, $elapsed, true, '', $rows ?? 0);
    } catch (\Throwable $e) {
        $elapsed = (hrtime(true) - $start) / 1e6;
        return new TestResult($name, $category, $elapsed, false, $e->getMessage());
    }
}

function q(mysqli $conn, string $sql): mysqli_result|bool {
    $result = $conn->query($sql);
    if ($result === false) {
        throw new \RuntimeException("Query failed: " . $conn->error . " | SQL: " . substr($sql, 0, 300));
    }
    return $result;
}

function q_val(mysqli $conn, string $sql): mixed {
    $r = q($conn, $sql);
    if ($r instanceof mysqli_result) {
        $row = $r->fetch_row();
        $r->free();
        return $row ? $row[0] : null;
    }
    return null;
}

function q_count(mysqli $conn, string $sql): int {
    $r = q($conn, $sql);
    if ($r instanceof mysqli_result) {
        $n = $r->num_rows;
        $r->free();
        return $n;
    }
    return 0;
}

// ===========================================================================
//
//  SECTION 1: CONNECTION INITIALIZATION QUERIES
//
//  These are the first SQL commands WordPress sends after connecting.
//  Source: wp-includes/class-wpdb.php  db_connect(), set_charset(),
//          set_sql_mode(), check_connection()
//
// ===========================================================================

function test_connection_init(mysqli $conn): array {
    $results = [];

    // WordPress sends SET NAMES on every connection
    $results[] = run_test($conn, "SET NAMES utf8mb4", "INIT", function($c) {
        q($c, "SET NAMES 'utf8mb4'");
        return 0;
    });

    // WordPress reads the current sql_mode then sets it
    $results[] = run_test($conn, "SELECT @@SESSION.sql_mode", "INIT", function($c) {
        $val = q_val($c, "SELECT @@SESSION.sql_mode");
        return $val !== null ? 1 : 0;
    });

    $results[] = run_test($conn, "SET SESSION sql_mode", "INIT", function($c) {
        q($c, "SET SESSION sql_mode=''");
        return 0;
    });

    // WordPress uses DO 1 to check the connection is alive
    $results[] = run_test($conn, "DO 1 (connection check)", "INIT", function($c) {
        q($c, "DO 1");
        return 0;
    });

    // WordPress checks charset support
    $results[] = run_test($conn, "SHOW FULL COLUMNS FROM (table info)", "INIT", function($c) {
        // This is called later on each table; tested after schema creation
        return 0;
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 2: CREATE TABLE DDL
//
//  Exact WordPress core table definitions from wp-admin/includes/schema.php
//  The $charset_collate would normally be:
//    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
//
// ===========================================================================

function test_schema_creation(mysqli $conn): array {
    $results = [];
    $charset = "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";

    // Drop all tables first to ensure clean state
    $tables = [
        'wp_termmeta', 'wp_term_relationships', 'wp_term_taxonomy', 'wp_terms',
        'wp_commentmeta', 'wp_comments', 'wp_links', 'wp_postmeta', 'wp_posts',
        'wp_options', 'wp_usermeta', 'wp_users',
    ];
    foreach ($tables as $t) {
        $conn->query("DROP TABLE IF EXISTS $t");
    }

    // ---- wp_users (must come before wp_posts due to logical dependency) ----
    $results[] = run_test($conn, "CREATE TABLE wp_users", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_users (
                ID bigint(20) unsigned NOT NULL auto_increment,
                user_login varchar(60) NOT NULL default '',
                user_pass varchar(255) NOT NULL default '',
                user_nicename varchar(50) NOT NULL default '',
                user_email varchar(100) NOT NULL default '',
                user_url varchar(100) NOT NULL default '',
                user_registered datetime NOT NULL default '0000-00-00 00:00:00',
                user_activation_key varchar(255) NOT NULL default '',
                user_status int(11) NOT NULL default '0',
                display_name varchar(250) NOT NULL default '',
                PRIMARY KEY  (ID),
                KEY user_login_key (user_login),
                KEY user_nicename (user_nicename),
                KEY user_email (user_email)
            ) $charset
        ");
        return 0;
    });

    // ---- wp_usermeta ----
    $results[] = run_test($conn, "CREATE TABLE wp_usermeta", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_usermeta (
                umeta_id bigint(20) unsigned NOT NULL auto_increment,
                user_id bigint(20) unsigned NOT NULL default '0',
                meta_key varchar(255) default NULL,
                meta_value longtext,
                PRIMARY KEY  (umeta_id),
                KEY user_id (user_id),
                KEY meta_key (meta_key(191))
            ) $charset
        ");
        return 0;
    });

    // ---- wp_posts ----
    $results[] = run_test($conn, "CREATE TABLE wp_posts", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_posts (
                ID bigint(20) unsigned NOT NULL auto_increment,
                post_author bigint(20) unsigned NOT NULL default '0',
                post_date datetime NOT NULL default '0000-00-00 00:00:00',
                post_date_gmt datetime NOT NULL default '0000-00-00 00:00:00',
                post_content longtext NOT NULL,
                post_title text NOT NULL,
                post_excerpt text NOT NULL,
                post_status varchar(20) NOT NULL default 'publish',
                comment_status varchar(20) NOT NULL default 'open',
                ping_status varchar(20) NOT NULL default 'open',
                post_password varchar(255) NOT NULL default '',
                post_name varchar(200) NOT NULL default '',
                to_ping text NOT NULL,
                pinged text NOT NULL,
                post_modified datetime NOT NULL default '0000-00-00 00:00:00',
                post_modified_gmt datetime NOT NULL default '0000-00-00 00:00:00',
                post_content_filtered longtext NOT NULL,
                post_parent bigint(20) unsigned NOT NULL default '0',
                guid varchar(255) NOT NULL default '',
                menu_order int(11) NOT NULL default '0',
                post_type varchar(20) NOT NULL default 'post',
                post_mime_type varchar(100) NOT NULL default '',
                comment_count bigint(20) NOT NULL default '0',
                PRIMARY KEY  (ID),
                KEY post_name (post_name(191)),
                KEY type_status_date (post_type,post_status,post_date,ID),
                KEY post_parent (post_parent),
                KEY post_author (post_author)
            ) $charset
        ");
        return 0;
    });

    // ---- wp_postmeta ----
    $results[] = run_test($conn, "CREATE TABLE wp_postmeta", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_postmeta (
                meta_id bigint(20) unsigned NOT NULL auto_increment,
                post_id bigint(20) unsigned NOT NULL default '0',
                meta_key varchar(255) default NULL,
                meta_value longtext,
                PRIMARY KEY  (meta_id),
                KEY post_id (post_id),
                KEY meta_key (meta_key(191))
            ) $charset
        ");
        return 0;
    });

    // ---- wp_comments ----
    $results[] = run_test($conn, "CREATE TABLE wp_comments", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_comments (
                comment_ID bigint(20) unsigned NOT NULL auto_increment,
                comment_post_ID bigint(20) unsigned NOT NULL default '0',
                comment_author tinytext NOT NULL,
                comment_author_email varchar(100) NOT NULL default '',
                comment_author_url varchar(200) NOT NULL default '',
                comment_author_IP varchar(100) NOT NULL default '',
                comment_date datetime NOT NULL default '0000-00-00 00:00:00',
                comment_date_gmt datetime NOT NULL default '0000-00-00 00:00:00',
                comment_content text NOT NULL,
                comment_karma int(11) NOT NULL default '0',
                comment_approved varchar(20) NOT NULL default '1',
                comment_agent varchar(255) NOT NULL default '',
                comment_type varchar(20) NOT NULL default 'comment',
                comment_parent bigint(20) unsigned NOT NULL default '0',
                user_id bigint(20) unsigned NOT NULL default '0',
                PRIMARY KEY  (comment_ID),
                KEY comment_post_ID (comment_post_ID),
                KEY comment_approved_date_gmt (comment_approved,comment_date_gmt),
                KEY comment_date_gmt (comment_date_gmt),
                KEY comment_parent (comment_parent),
                KEY comment_author_email (comment_author_email(10))
            ) $charset
        ");
        return 0;
    });

    // ---- wp_commentmeta ----
    $results[] = run_test($conn, "CREATE TABLE wp_commentmeta", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_commentmeta (
                meta_id bigint(20) unsigned NOT NULL auto_increment,
                comment_id bigint(20) unsigned NOT NULL default '0',
                meta_key varchar(255) default NULL,
                meta_value longtext,
                PRIMARY KEY  (meta_id),
                KEY comment_id (comment_id),
                KEY meta_key (meta_key(191))
            ) $charset
        ");
        return 0;
    });

    // ---- wp_terms ----
    $results[] = run_test($conn, "CREATE TABLE wp_terms", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_terms (
                term_id bigint(20) unsigned NOT NULL auto_increment,
                name varchar(200) NOT NULL default '',
                slug varchar(200) NOT NULL default '',
                term_group bigint(10) NOT NULL default 0,
                PRIMARY KEY  (term_id),
                KEY slug (slug(191)),
                KEY name (name(191))
            ) $charset
        ");
        return 0;
    });

    // ---- wp_term_taxonomy ----
    $results[] = run_test($conn, "CREATE TABLE wp_term_taxonomy", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_term_taxonomy (
                term_taxonomy_id bigint(20) unsigned NOT NULL auto_increment,
                term_id bigint(20) unsigned NOT NULL default 0,
                taxonomy varchar(32) NOT NULL default '',
                description longtext NOT NULL,
                parent bigint(20) unsigned NOT NULL default 0,
                count bigint(20) NOT NULL default 0,
                PRIMARY KEY  (term_taxonomy_id),
                UNIQUE KEY term_id_taxonomy (term_id,taxonomy),
                KEY taxonomy (taxonomy)
            ) $charset
        ");
        return 0;
    });

    // ---- wp_term_relationships ----
    $results[] = run_test($conn, "CREATE TABLE wp_term_relationships", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_term_relationships (
                object_id bigint(20) unsigned NOT NULL default 0,
                term_taxonomy_id bigint(20) unsigned NOT NULL default 0,
                term_order int(11) NOT NULL default 0,
                PRIMARY KEY  (object_id,term_taxonomy_id),
                KEY term_taxonomy_id (term_taxonomy_id)
            ) $charset
        ");
        return 0;
    });

    // ---- wp_termmeta ----
    $results[] = run_test($conn, "CREATE TABLE wp_termmeta", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_termmeta (
                meta_id bigint(20) unsigned NOT NULL auto_increment,
                term_id bigint(20) unsigned NOT NULL default '0',
                meta_key varchar(255) default NULL,
                meta_value longtext,
                PRIMARY KEY  (meta_id),
                KEY term_id (term_id),
                KEY meta_key (meta_key(191))
            ) $charset
        ");
        return 0;
    });

    // ---- wp_options ----
    $results[] = run_test($conn, "CREATE TABLE wp_options", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_options (
                option_id bigint(20) unsigned NOT NULL auto_increment,
                option_name varchar(191) NOT NULL default '',
                option_value longtext NOT NULL,
                autoload varchar(20) NOT NULL default 'yes',
                PRIMARY KEY  (option_id),
                UNIQUE KEY option_name (option_name),
                KEY autoload (autoload)
            ) $charset
        ");
        return 0;
    });

    // ---- wp_links ----
    $results[] = run_test($conn, "CREATE TABLE wp_links", "DDL", function($c) use ($charset) {
        q($c, "
            CREATE TABLE wp_links (
                link_id bigint(20) unsigned NOT NULL auto_increment,
                link_url varchar(255) NOT NULL default '',
                link_name varchar(255) NOT NULL default '',
                link_image varchar(255) NOT NULL default '',
                link_target varchar(25) NOT NULL default '',
                link_description varchar(255) NOT NULL default '',
                link_visible varchar(20) NOT NULL default 'Y',
                link_owner bigint(20) unsigned NOT NULL default '1',
                link_rating int(11) NOT NULL default '0',
                link_updated datetime NOT NULL default '0000-00-00 00:00:00',
                link_rel varchar(255) NOT NULL default '',
                link_notes mediumtext NOT NULL,
                link_rss varchar(255) NOT NULL default '',
                PRIMARY KEY  (link_id),
                KEY link_visible (link_visible)
            ) $charset
        ");
        return 0;
    });

    // ---- SHOW FULL COLUMNS (WordPress calls this per-table for charset detection) ----
    foreach (['wp_posts', 'wp_options', 'wp_users', 'wp_comments'] as $tbl) {
        $results[] = run_test($conn, "SHOW FULL COLUMNS FROM $tbl", "DDL", function($c) use ($tbl) {
            return q_count($c, "SHOW FULL COLUMNS FROM `$tbl`");
        });
    }

    // ---- SHOW TABLES (WordPress checks which tables exist) ----
    $results[] = run_test($conn, "SHOW TABLES LIKE 'wp_%'", "DDL", function($c) {
        return q_count($c, "SHOW TABLES LIKE 'wp_%'");
    });

    // ---- DESCRIBE (used by maybe_add_column) ----
    $results[] = run_test($conn, "DESCRIBE wp_posts", "DDL", function($c) {
        return q_count($c, "DESCRIBE wp_posts");
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 3: WORDPRESS INSTALLATION DATA (wp_install_defaults)
//
//  Exact INSERTs WordPress runs during fresh install.
//  Source: wp-admin/includes/upgrade.php  wp_install_defaults()
//
// ===========================================================================

function test_install_data(mysqli $conn): array {
    $results = [];
    $now = '2024-01-01 00:00:00';
    $now_gmt = '2024-01-01 00:00:00';

    // ---- Create admin user (wp_install) ----
    $results[] = run_test($conn, "INSERT admin user", "INSTALL", function($c) use ($now) {
        q($c, "INSERT INTO wp_users (user_login, user_pass, user_nicename, user_email, user_url, user_registered, user_activation_key, user_status, display_name) VALUES ('admin', '\$P\$BHash.Placeholder.000000000000000', 'admin', 'admin@example.com', 'http://localhost', '$now', '', 0, 'admin')");
        return 1;
    });

    // ---- Insert admin usermeta ----
    $results[] = run_test($conn, "INSERT admin usermeta", "INSTALL", function($c) {
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'wp_capabilities', 'a:1:{s:13:\"administrator\";b:1;}')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'wp_user_level', '10')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'nickname', 'admin')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'first_name', '')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'last_name', '')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'description', '')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'rich_editing', 'true')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'syntax_highlighting', 'true')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'comment_shortcuts', 'false')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'admin_color', 'fresh')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'use_ssl', '0')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'show_admin_bar_front', 'true')");
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'locale', '')");
        return 13;
    });

    // ---- Default category (Uncategorized) ----
    $results[] = run_test($conn, "INSERT default category", "INSTALL", function($c) {
        q($c, "INSERT INTO wp_terms (term_id, name, slug, term_group) VALUES (1, 'Uncategorized', 'uncategorized', 0)");
        q($c, "INSERT INTO wp_term_taxonomy (term_id, taxonomy, description, parent, count) VALUES (1, 'category', '', 0, 1)");
        return 2;
    });

    // ---- Default options (core WordPress settings) ----
    $results[] = run_test($conn, "INSERT core options (50+)", "INSTALL", function($c) {
        $options = [
            ['siteurl', 'http://localhost'],
            ['home', 'http://localhost'],
            ['blogname', 'Test Site'],
            ['blogdescription', 'Just another WordPress site'],
            ['users_can_register', '0'],
            ['admin_email', 'admin@example.com'],
            ['start_of_week', '1'],
            ['use_balanceTags', '0'],
            ['use_smilies', '1'],
            ['require_name_email', '1'],
            ['comments_notify', '1'],
            ['posts_per_rss', '10'],
            ['rss_use_excerpt', '0'],
            ['mailserver_url', 'mail.example.com'],
            ['mailserver_login', 'login@example.com'],
            ['mailserver_pass', 'password'],
            ['mailserver_port', '110'],
            ['default_category', '1'],
            ['default_comment_status', 'open'],
            ['default_ping_status', 'open'],
            ['default_pingback_flag', '1'],
            ['posts_per_page', '10'],
            ['date_format', 'F j, Y'],
            ['time_format', 'g:i a'],
            ['links_updated_date_format', 'F j, Y g:i a'],
            ['comment_moderation', '0'],
            ['moderation_notify', '1'],
            ['permalink_structure', '/%year%/%monthnum%/%day%/%postname%/'],
            ['rewrite_rules', ''],
            ['hack_file', '0'],
            ['blog_charset', 'UTF-8'],
            ['moderation_keys', ''],
            ['active_plugins', 'a:0:{}'],
            ['category_base', ''],
            ['ping_sites', 'http://rpc.pingomatic.com/'],
            ['comment_max_links', '2'],
            ['gmt_offset', '0'],
            ['default_email_category', '1'],
            ['recently_edited', ''],
            ['template', 'twentytwentyfour'],
            ['stylesheet', 'twentytwentyfour'],
            ['comment_registration', '0'],
            ['html_type', 'text/html'],
            ['use_trackback', '0'],
            ['default_role', 'subscriber'],
            ['db_version', '57155'],
            ['uploads_use_yearmonth_folders', '1'],
            ['upload_path', ''],
            ['blog_public', '1'],
            ['default_link_category', '2'],
            ['show_on_front', 'posts'],
            ['tag_base', ''],
            ['show_avatars', '1'],
            ['avatar_rating', 'G'],
            ['upload_url_path', ''],
            ['thumbnail_size_w', '150'],
            ['thumbnail_size_h', '150'],
            ['thumbnail_crop', '1'],
            ['medium_size_w', '300'],
            ['medium_size_h', '300'],
            ['avatar_default', 'mystery'],
            ['large_size_w', '1024'],
            ['large_size_h', '1024'],
            ['image_default_link_type', 'none'],
            ['image_default_size', ''],
            ['image_default_align', ''],
            ['sidebars_widgets', 'a:0:{}'],
            ['cron', 'a:0:{}'],
            ['widget_categories', 'a:0:{}'],
            ['widget_text', 'a:0:{}'],
            ['widget_rss', 'a:0:{}'],
            ['uninstall_plugins', 'a:0:{}'],
            ['timezone_string', ''],
            ['page_for_posts', '0'],
            ['page_on_front', '0'],
            ['default_post_format', '0'],
            ['link_manager_enabled', '0'],
            ['finished_splitting_shared_terms', '1'],
            ['site_icon', '0'],
            ['medium_large_size_w', '768'],
            ['medium_large_size_h', '0'],
            ['wp_page_for_privacy_policy', '0'],
            ['show_comments_cookies_opt_in', '1'],
            ['admin_email_lifespan', '0'],
            ['disallowed_keys', ''],
            ['comment_previously_approved', '1'],
            ['auto_plugin_theme_update_emails', 'a:0:{}'],
            ['auto_update_core_dev', 'enabled'],
            ['auto_update_core_minor', 'enabled'],
            ['auto_update_core_major', 'unset'],
            ['initial_db_version', '57155'],
            ['wp_user_roles', 'a:5:{s:13:"administrator";a:2:{s:4:"name";s:13:"Administrator";s:12:"capabilities";a:0:{}}s:6:"editor";a:2:{s:4:"name";s:6:"Editor";s:12:"capabilities";a:0:{}}s:6:"author";a:2:{s:4:"name";s:6:"Author";s:12:"capabilities";a:0:{}}s:11:"contributor";a:2:{s:4:"name";s:11:"Contributor";s:12:"capabilities";a:0:{}}s:10:"subscriber";a:2:{s:4:"name";s:10:"Subscriber";s:12:"capabilities";a:0:{}}}'],
            ['fresh_site', '1'],
            ['auto_update_plugins', 'a:0:{}'],
            ['auto_update_themes', 'a:0:{}'],
        ];
        // WordPress uses INSERT ... ON DUPLICATE KEY UPDATE for options
        $count = 0;
        foreach ($options as [$name, $value]) {
            $name_esc = $c->real_escape_string($name);
            $val_esc = $c->real_escape_string($value);
            q($c, "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('$name_esc', '$val_esc', 'yes') ON DUPLICATE KEY UPDATE option_name = VALUES(option_name), option_value = VALUES(option_value), autoload = VALUES(autoload)");
            $count++;
        }
        return $count;
    });

    // ---- Default "Hello World" post ----
    $results[] = run_test($conn, "INSERT default post (Hello World)", "INSTALL", function($c) use ($now, $now_gmt) {
        $content = $c->real_escape_string("Welcome to WordPress. This is your first post. Edit or delete it, then start writing!");
        q($c, "INSERT INTO wp_posts (post_author, post_date, post_date_gmt, post_content, post_excerpt, post_title, post_name, post_modified, post_modified_gmt, guid, comment_count, to_ping, pinged, post_content_filtered, post_status, comment_status, ping_status, post_password, post_type, post_mime_type) VALUES (1, '$now', '$now_gmt', '$content', '', 'Hello world!', 'hello-world', '$now', '$now_gmt', 'http://localhost/?p=1', 1, '', '', '', 'publish', 'open', 'open', '', 'post', '')");
        // Link post to default category
        q($c, "INSERT INTO wp_term_relationships (object_id, term_taxonomy_id) VALUES (1, 1)");
        return 2;
    });

    // ---- Default comment on Hello World ----
    $results[] = run_test($conn, "INSERT default comment", "INSTALL", function($c) use ($now, $now_gmt) {
        $content = $c->real_escape_string("Hi, this is a comment.\nTo get started with moderating, editing, and deleting comments, please visit the Comments screen in the dashboard.\nCommenter avatars come from Gravatar.");
        q($c, "INSERT INTO wp_comments (comment_post_ID, comment_author, comment_author_email, comment_author_url, comment_date, comment_date_gmt, comment_content, comment_approved, comment_agent, comment_type, comment_parent, user_id, comment_author_IP, comment_karma) VALUES (1, 'A WordPress Commenter', 'wapuu@wordpress.example', 'https://wordpress.org/', '$now', '$now_gmt', '$content', '1', '', 'comment', 0, 0, '', 0)");
        return 1;
    });

    // ---- Default Sample Page ----
    $results[] = run_test($conn, "INSERT default Sample Page", "INSTALL", function($c) use ($now, $now_gmt) {
        $content = $c->real_escape_string("This is an example page. It's different from a blog post because it will stay in one place and will show up in your site navigation.");
        q($c, "INSERT INTO wp_posts (post_author, post_date, post_date_gmt, post_content, post_excerpt, post_title, post_name, post_modified, post_modified_gmt, guid, to_ping, pinged, post_content_filtered, post_status, comment_status, ping_status, post_password, post_type, post_mime_type, comment_count) VALUES (1, '$now', '$now_gmt', '$content', '', 'Sample Page', 'sample-page', '$now', '$now_gmt', 'http://localhost/?page_id=2', '', '', '', 'publish', 'closed', 'open', '', 'page', '', 0)");
        q($c, "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (2, '_wp_page_template', 'default')");
        return 2;
    });

    // ---- Default Privacy Policy page ----
    $results[] = run_test($conn, "INSERT default Privacy Policy page", "INSTALL", function($c) use ($now, $now_gmt) {
        $content = $c->real_escape_string("This is the privacy policy page content placeholder.");
        q($c, "INSERT INTO wp_posts (post_author, post_date, post_date_gmt, post_content, post_excerpt, post_title, post_name, post_modified, post_modified_gmt, guid, to_ping, pinged, post_content_filtered, post_status, comment_status, ping_status, post_password, post_type, post_mime_type, comment_count) VALUES (1, '$now', '$now_gmt', '$content', '', 'Privacy Policy', 'privacy-policy', '$now', '$now_gmt', 'http://localhost/?page_id=3', '', '', '', 'draft', 'closed', 'open', '', 'page', '', 0)");
        q($c, "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (3, '_wp_page_template', 'default')");
        return 2;
    });

    // ---- Update options that reference install data ----
    $results[] = run_test($conn, "UPDATE options post-install", "INSTALL", function($c) {
        q($c, "UPDATE wp_options SET option_value = '3' WHERE option_name = 'wp_page_for_privacy_policy'");
        q($c, "UPDATE wp_options SET option_value = '0' WHERE option_name = 'fresh_site'");
        return 2;
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 4: OPTIONS API QUERIES
//
//  Exact queries from wp-includes/option.php:
//    get_option(), update_option(), add_option(), delete_option(),
//    wp_load_alloptions(), wp_prime_option_caches()
//
// ===========================================================================

function test_options_queries(mysqli $conn): array {
    $results = [];

    // ---- wp_load_alloptions(): load all autoloaded options at once ----
    $results[] = run_test($conn, "SELECT all autoloaded options", "OPTIONS", function($c) {
        return q_count($c, "SELECT option_name, option_value FROM wp_options WHERE autoload IN ('yes', 'on', 'auto-on')");
    });

    // ---- get_option(): point lookup by option_name ----
    $results[] = run_test($conn, "get_option() siteurl", "OPTIONS", function($c) {
        return q_count($c, "SELECT option_value FROM wp_options WHERE option_name = 'siteurl' LIMIT 1");
    });

    $results[] = run_test($conn, "get_option() blogname", "OPTIONS", function($c) {
        return q_count($c, "SELECT option_value FROM wp_options WHERE option_name = 'blogname' LIMIT 1");
    });

    $results[] = run_test($conn, "get_option() template", "OPTIONS", function($c) {
        return q_count($c, "SELECT option_value FROM wp_options WHERE option_name = 'template' LIMIT 1");
    });

    $results[] = run_test($conn, "get_option() stylesheet", "OPTIONS", function($c) {
        return q_count($c, "SELECT option_value FROM wp_options WHERE option_name = 'stylesheet' LIMIT 1");
    });

    $results[] = run_test($conn, "get_option() active_plugins", "OPTIONS", function($c) {
        return q_count($c, "SELECT option_value FROM wp_options WHERE option_name = 'active_plugins' LIMIT 1");
    });

    $results[] = run_test($conn, "get_option() permalink_structure", "OPTIONS", function($c) {
        return q_count($c, "SELECT option_value FROM wp_options WHERE option_name = 'permalink_structure' LIMIT 1");
    });

    // ---- wp_prime_option_caches(): batch option lookup ----
    $results[] = run_test($conn, "Batch SELECT options IN(...)", "OPTIONS", function($c) {
        return q_count($c, "SELECT option_name, option_value FROM wp_options WHERE option_name IN ('siteurl', 'home', 'blogname', 'blogdescription', 'admin_email', 'template', 'stylesheet', 'date_format', 'time_format', 'posts_per_page')");
    });

    // ---- update_option(): update existing option ----
    $results[] = run_test($conn, "update_option() blogname", "OPTIONS", function($c) {
        q($c, "SELECT autoload FROM wp_options WHERE option_name = 'blogname' LIMIT 1");
        q($c, "UPDATE wp_options SET option_value = 'Updated Site Name' WHERE option_name = 'blogname'");
        return 1;
    });

    // ---- add_option(): INSERT with ON DUPLICATE KEY ----
    $results[] = run_test($conn, "add_option() new transient", "OPTIONS", function($c) {
        q($c, "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('_transient_test_key', 'transient_value_123', 'no') ON DUPLICATE KEY UPDATE option_name = VALUES(option_name), option_value = VALUES(option_value), autoload = VALUES(autoload)");
        return 1;
    });

    // ---- delete_option(): remove by name ----
    $results[] = run_test($conn, "delete_option() transient", "OPTIONS", function($c) {
        q($c, "SELECT autoload FROM wp_options WHERE option_name = '_transient_test_key'");
        q($c, "DELETE FROM wp_options WHERE option_name = '_transient_test_key'");
        return 1;
    });

    // ---- Transient cleanup (delete_expired_transients) ----
    $results[] = run_test($conn, "INSERT+DELETE transient lifecycle", "OPTIONS", function($c) {
        // Simulate transient with timeout
        q($c, "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('_transient_timeout_feed_abc', '1700000000', 'no') ON DUPLICATE KEY UPDATE option_name = VALUES(option_name), option_value = VALUES(option_value), autoload = VALUES(autoload)");
        q($c, "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('_transient_feed_abc', 'cached_feed_data', 'no') ON DUPLICATE KEY UPDATE option_name = VALUES(option_name), option_value = VALUES(option_value), autoload = VALUES(autoload)");
        // WordPress transient cleanup query pattern:
        // DELETE a, b FROM wp_options a, wp_options b
        //   WHERE a.option_name LIKE '_transient_%'
        //   AND a.option_name NOT LIKE '_transient_timeout_%'
        //   AND b.option_name = CONCAT('_transient_timeout_', SUBSTRING(a.option_name, 12))
        //   AND b.option_value < UNIX_TIMESTAMP()
        q($c, "DELETE a, b FROM wp_options a, wp_options b WHERE a.option_name LIKE '_transient_%' AND a.option_name NOT LIKE '_transient_timeout_%' AND b.option_name = CONCAT('_transient_timeout_', SUBSTRING(a.option_name, 12)) AND b.option_value < 2000000000");
        return 1;
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 5: POST QUERIES (WP_Query patterns)
//
//  Exact queries from wp-includes/class-wp-query.php get_posts() and
//  wp-includes/post.php operations
//
// ===========================================================================

function test_post_queries(mysqli $conn): array {
    $results = [];

    // Insert more varied test posts first
    $results[] = run_test($conn, "INSERT 50 varied posts", "POSTS", function($c) {
        $statuses = ['publish', 'draft', 'private', 'pending', 'trash', 'future'];
        $types = ['post', 'page', 'attachment', 'revision', 'nav_menu_item'];
        $count = 0;
        for ($i = 4; $i <= 53; $i++) {
            $author = ($i % 3) + 1;
            $status = $statuses[$i % 6];
            $type = $types[$i % 5];
            $day = str_pad(($i % 28) + 1, 2, '0', STR_PAD_LEFT);
            $month = str_pad(($i % 12) + 1, 2, '0', STR_PAD_LEFT);
            $date = "2024-$month-$day 12:00:00";
            $title = addslashes("Test Post $i: A sample title with keywords");
            $content = addslashes("This is the content of post $i. It contains various words for searching. WordPress is a content management system.");
            $name = "test-post-$i";
            $guid = "http://localhost/?p=$i";
            q($c, "INSERT INTO wp_posts (post_author, post_date, post_date_gmt, post_content, post_excerpt, post_title, post_name, post_modified, post_modified_gmt, guid, to_ping, pinged, post_content_filtered, post_status, comment_status, ping_status, post_password, post_type, post_mime_type, comment_count, post_parent, menu_order) VALUES ($author, '$date', '$date', '$content', '', '$title', '$name', '$date', '$date', '$guid', '', '', '', '$status', 'open', 'open', '', '$type', '', 0, 0, 0)");
            $count++;
        }
        return $count;
    });

    // ---- Basic WP_Query: SELECT published posts ----
    $results[] = run_test($conn, "WP_Query: published posts", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.ID FROM wp_posts WHERE 1=1 AND wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish' ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    // ---- WP_Query: all fields, type+status+date compound index ----
    $results[] = run_test($conn, "WP_Query: full post objects", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND wp_posts.post_type = 'post' AND (wp_posts.post_status = 'publish' OR wp_posts.post_status = 'private') ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    // ---- WP_Query: pages ----
    $results[] = run_test($conn, "WP_Query: pages", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND wp_posts.post_type = 'page' AND wp_posts.post_status = 'publish' ORDER BY wp_posts.post_title ASC");
    });

    // ---- WP_Query: by author ----
    $results[] = run_test($conn, "WP_Query: by author", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND wp_posts.post_author IN (1) AND wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish' ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    // ---- WP_Query: date query ----
    $results[] = run_test($conn, "WP_Query: date filter", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND YEAR(wp_posts.post_date) = 2024 AND MONTH(wp_posts.post_date) = 1 AND wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish' ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    // ---- WP_Query: search (LIKE) ----
    $results[] = run_test($conn, "WP_Query: search LIKE", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND (wp_posts.post_title LIKE '%WordPress%' OR wp_posts.post_content LIKE '%WordPress%' OR wp_posts.post_excerpt LIKE '%WordPress%') AND wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish' ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    // ---- WP_Query: by slug (post_name) ----
    $results[] = run_test($conn, "WP_Query: by post_name", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND wp_posts.post_name = 'hello-world' AND wp_posts.post_type = 'post' LIMIT 1");
    });

    // ---- WP_Query: by multiple post IDs ----
    $results[] = run_test($conn, "WP_Query: post__in", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND wp_posts.ID IN (1, 2, 3, 4, 5) AND wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish' ORDER BY FIELD(wp_posts.ID, 1, 2, 3, 4, 5)");
    });

    // ---- WP_Query: COUNT(*) for pagination ----
    $results[] = run_test($conn, "WP_Query: count for pagination", "POSTS", function($c) {
        $val = q_val($c, "SELECT COUNT(*) FROM wp_posts WHERE 1=1 AND wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish'");
        return (int)$val;
    });

    // ---- WP_Query: posts + taxonomy JOIN ----
    $results[] = run_test($conn, "WP_Query: taxonomy JOIN", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts INNER JOIN wp_term_relationships ON (wp_posts.ID = wp_term_relationships.object_id) WHERE 1=1 AND wp_term_relationships.term_taxonomy_id IN (1) AND wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish' GROUP BY wp_posts.ID ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    // ---- WP_Query: posts + meta JOIN ----
    $results[] = run_test($conn, "WP_Query: meta_query JOIN", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts INNER JOIN wp_postmeta ON (wp_posts.ID = wp_postmeta.post_id) WHERE 1=1 AND wp_postmeta.meta_key = '_wp_page_template' AND wp_postmeta.meta_value = 'default' AND wp_posts.post_type = 'page' AND wp_posts.post_status = 'publish' GROUP BY wp_posts.ID ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    // ---- WP_Query: posts + users JOIN (admin queries) ----
    $results[] = run_test($conn, "WP_Query: posts JOIN users", "POSTS", function($c) {
        return q_count($c, "SELECT wp_posts.post_title, wp_users.display_name FROM wp_posts INNER JOIN wp_users ON wp_posts.post_author = wp_users.ID WHERE wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish' ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    // ---- Single post by ID (get_post / WP_Post::get_instance) ----
    $results[] = run_test($conn, "get_post() by ID", "POSTS", function($c) {
        return q_count($c, "SELECT * FROM wp_posts WHERE ID = 1 LIMIT 1");
    });

    // ---- wp_insert_post: UPDATE existing post ----
    $results[] = run_test($conn, "wp_update_post()", "POSTS", function($c) {
        q($c, "UPDATE wp_posts SET post_title = 'Hello world! (Updated)', post_modified = '2024-06-15 14:00:00', post_modified_gmt = '2024-06-15 14:00:00' WHERE ID = 1");
        return 1;
    });

    // ---- wp_trash_post ----
    $results[] = run_test($conn, "wp_trash_post()", "POSTS", function($c) {
        q($c, "UPDATE wp_posts SET post_status = 'trash' WHERE ID = 4");
        q($c, "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (4, '_wp_trash_meta_status', 'publish')");
        q($c, "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (4, '_wp_trash_meta_time', '1700000000')");
        return 1;
    });

    // ---- wp_delete_post ----
    $results[] = run_test($conn, "wp_delete_post()", "POSTS", function($c) {
        q($c, "DELETE FROM wp_term_relationships WHERE object_id = 4");
        q($c, "DELETE FROM wp_postmeta WHERE post_id = 4");
        q($c, "DELETE FROM wp_posts WHERE ID = 4");
        return 1;
    });

    // ---- Adjacent post queries (previous/next post) ----
    $results[] = run_test($conn, "get_adjacent_post() next", "POSTS", function($c) {
        return q_count($c, "SELECT p.ID FROM wp_posts AS p WHERE p.post_date > '2024-01-01 12:00:00' AND p.post_type = 'post' AND p.post_status = 'publish' ORDER BY p.post_date ASC LIMIT 1");
    });

    $results[] = run_test($conn, "get_adjacent_post() prev", "POSTS", function($c) {
        return q_count($c, "SELECT p.ID FROM wp_posts AS p WHERE p.post_date < '2024-12-28 12:00:00' AND p.post_type = 'post' AND p.post_status = 'publish' ORDER BY p.post_date DESC LIMIT 1");
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 6: META API QUERIES
//
//  Exact queries from wp-includes/meta.php:
//    add_metadata(), update_metadata(), get_metadata_raw(),
//    delete_metadata(), update_meta_cache()
//
// ===========================================================================

function test_meta_queries(mysqli $conn): array {
    $results = [];

    // ---- add_metadata (postmeta) ----
    $results[] = run_test($conn, "add_post_meta()", "META", function($c) {
        q($c, "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (1, '_edit_lock', '1700000000:1')");
        q($c, "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (1, '_edit_last', '1')");
        q($c, "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (1, '_thumbnail_id', '10')");
        return 3;
    });

    // ---- get_metadata (check exists) ----
    $results[] = run_test($conn, "get_post_meta() check exists", "META", function($c) {
        $val = q_val($c, "SELECT COUNT(*) FROM wp_postmeta WHERE meta_key = '_edit_lock' AND post_id = 1");
        return (int)$val;
    });

    // ---- update_meta_cache: bulk load all meta for a list of post IDs ----
    $results[] = run_test($conn, "update_meta_cache() bulk load", "META", function($c) {
        return q_count($c, "SELECT post_id, meta_key, meta_value FROM wp_postmeta WHERE post_id IN (1, 2, 3, 5, 6, 7, 8, 9, 10) ORDER BY meta_id ASC");
    });

    // ---- update_metadata ----
    $results[] = run_test($conn, "update_post_meta()", "META", function($c) {
        q($c, "UPDATE wp_postmeta SET meta_value = '1700001000:1' WHERE post_id = 1 AND meta_key = '_edit_lock'");
        return 1;
    });

    // ---- get_metadata_by_mid ----
    $results[] = run_test($conn, "get_metadata_by_mid()", "META", function($c) {
        return q_count($c, "SELECT * FROM wp_postmeta WHERE meta_id = 1");
    });

    // ---- delete_metadata ----
    $results[] = run_test($conn, "delete_post_meta()", "META", function($c) {
        // WordPress first gets meta_ids, then deletes
        q($c, "SELECT meta_id FROM wp_postmeta WHERE meta_key = '_edit_lock' AND post_id = 1");
        q($c, "DELETE FROM wp_postmeta WHERE meta_id IN (SELECT meta_id FROM wp_postmeta WHERE meta_key = '_edit_lock' AND post_id = 1)");
        return 1;
    });

    // ---- usermeta operations ----
    $results[] = run_test($conn, "add_user_meta()", "META", function($c) {
        q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES (1, 'session_tokens', 'a:1:{s:64:\"abc\";a:4:{s:10:\"expiration\";i:1700000000;s:2:\"ip\";s:9:\"127.0.0.1\";s:2:\"ua\";s:0:\"\";s:5:\"login\";i:1700000000;}}')");
        return 1;
    });

    $results[] = run_test($conn, "get_user_meta() bulk", "META", function($c) {
        return q_count($c, "SELECT user_id, meta_key, meta_value FROM wp_usermeta WHERE user_id IN (1) ORDER BY umeta_id ASC");
    });

    // ---- commentmeta ----
    $results[] = run_test($conn, "add_comment_meta()", "META", function($c) {
        q($c, "INSERT INTO wp_commentmeta (comment_id, meta_key, meta_value) VALUES (1, '_wp_trash_meta_status', '1')");
        return 1;
    });

    $results[] = run_test($conn, "get_comment_meta() bulk", "META", function($c) {
        return q_count($c, "SELECT comment_id, meta_key, meta_value FROM wp_commentmeta WHERE comment_id IN (1) ORDER BY meta_id ASC");
    });

    // ---- termmeta ----
    $results[] = run_test($conn, "add_term_meta()", "META", function($c) {
        q($c, "INSERT INTO wp_termmeta (term_id, meta_key, meta_value) VALUES (1, 'order', '0')");
        return 1;
    });

    $results[] = run_test($conn, "get_term_meta() bulk", "META", function($c) {
        return q_count($c, "SELECT term_id, meta_key, meta_value FROM wp_termmeta WHERE term_id IN (1) ORDER BY meta_id ASC");
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 7: USER QUERIES
//
//  Exact queries from wp-includes/user.php and wp-includes/class-wpdb.php
//
// ===========================================================================

function test_user_queries(mysqli $conn): array {
    $results = [];

    // ---- Insert more test users ----
    $results[] = run_test($conn, "INSERT 10 users + usermeta", "USERS", function($c) {
        $count = 0;
        for ($i = 2; $i <= 11; $i++) {
            $login = "user$i";
            $email = "user$i@example.com";
            q($c, "INSERT INTO wp_users (user_login, user_pass, user_nicename, user_email, user_url, user_registered, user_activation_key, user_status, display_name) VALUES ('$login', '\$P\$BHash.Placeholder.000000000000000', '$login', '$email', '', '2024-01-01 00:00:00', '', 0, 'User $i')");
            // WordPress always inserts capabilities and user_level
            q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES ($i, 'wp_capabilities', 'a:1:{s:10:\"subscriber\";b:1;}')");
            q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES ($i, 'wp_user_level', '0')");
            q($c, "INSERT INTO wp_usermeta (user_id, meta_key, meta_value) VALUES ($i, 'nickname', '$login')");
            $count++;
        }
        return $count;
    });

    // ---- get_user_by('login') -- used in wp_authenticate ----
    $results[] = run_test($conn, "get_user_by('login')", "USERS", function($c) {
        return q_count($c, "SELECT * FROM wp_users WHERE user_login = 'admin'");
    });

    // ---- get_user_by('email') ----
    $results[] = run_test($conn, "get_user_by('email')", "USERS", function($c) {
        return q_count($c, "SELECT * FROM wp_users WHERE user_email = 'admin@example.com'");
    });

    // ---- get_user_by('id') ----
    $results[] = run_test($conn, "get_user_by('id')", "USERS", function($c) {
        return q_count($c, "SELECT * FROM wp_users WHERE ID = 1");
    });

    // ---- get_user_by('slug') / nicename ----
    $results[] = run_test($conn, "get_user_by('slug')", "USERS", function($c) {
        return q_count($c, "SELECT * FROM wp_users WHERE user_nicename = 'admin'");
    });

    // ---- Check unique nicename (wp_insert_user) ----
    $results[] = run_test($conn, "Check unique user_nicename", "USERS", function($c) {
        return q_count($c, "SELECT ID FROM wp_users WHERE user_nicename = 'admin' AND user_login != 'admin' LIMIT 1");
    });

    // ---- count_users() ----
    $results[] = run_test($conn, "count_users()", "USERS", function($c) {
        $val = q_val($c, "SELECT COUNT(*) FROM wp_usermeta INNER JOIN wp_users ON user_id = ID WHERE meta_key = 'wp_capabilities'");
        return (int)$val;
    });

    // ---- wp_update_user_counts (network) ----
    $results[] = run_test($conn, "Count all users", "USERS", function($c) {
        $val = q_val($c, "SELECT COUNT(ID) as c FROM wp_users WHERE user_status = 0");
        return (int)$val;
    });

    // ---- Update user ----
    $results[] = run_test($conn, "wp_update_user() display_name", "USERS", function($c) {
        q($c, "UPDATE wp_users SET display_name = 'Site Administrator' WHERE ID = 1");
        return 1;
    });

    // ---- Clear activation key (upgrade_252 pattern) ----
    $results[] = run_test($conn, "Clear user activation keys", "USERS", function($c) {
        q($c, "UPDATE wp_users SET user_activation_key = '' WHERE ID > 0");
        return 1;
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 8: COMMENT QUERIES
//
//  Exact queries from wp-includes/comment.php
//
// ===========================================================================

function test_comment_queries(mysqli $conn): array {
    $results = [];

    // ---- Insert test comments ----
    $results[] = run_test($conn, "INSERT 20 comments", "COMMENTS", function($c) {
        $count = 0;
        for ($i = 2; $i <= 21; $i++) {
            $post_id = ($i % 10) + 1;
            $approved = $i % 3 === 0 ? '0' : '1';
            $type = $i % 5 === 0 ? 'pingback' : 'comment';
            $day = str_pad(($i % 28) + 1, 2, '0', STR_PAD_LEFT);
            $date = "2024-03-$day 10:00:00";
            $author = addslashes("Commenter $i");
            $content = addslashes("This is comment number $i on post $post_id.");
            $email = "commenter$i@example.com";
            q($c, "INSERT INTO wp_comments (comment_post_ID, comment_author, comment_author_email, comment_author_url, comment_date, comment_date_gmt, comment_content, comment_approved, comment_agent, comment_type, comment_parent, user_id, comment_author_IP, comment_karma) VALUES ($post_id, '$author', '$email', '', '$date', '$date', '$content', '$approved', 'Mozilla/5.0', '$type', 0, 0, '127.0.0.1', 0)");
            $count++;
        }
        return $count;
    });

    // ---- Check prior approval (wp_allow_comment) ----
    $results[] = run_test($conn, "Check prior comment approval", "COMMENTS", function($c) {
        return q_count($c, "SELECT comment_approved FROM wp_comments WHERE comment_author = 'Commenter 2' AND comment_author_email = 'commenter2@example.com' AND comment_approved = '1' LIMIT 1");
    });

    // ---- Get child comments ----
    $results[] = run_test($conn, "Get child comments", "COMMENTS", function($c) {
        return q_count($c, "SELECT comment_ID FROM wp_comments WHERE comment_parent = 1");
    });

    // ---- wp_update_comment_count_now ----
    $results[] = run_test($conn, "Count approved comments per post", "COMMENTS", function($c) {
        $val = q_val($c, "SELECT COUNT(*) FROM wp_comments WHERE comment_post_ID = 1 AND comment_approved = '1'");
        q($c, "UPDATE wp_posts SET comment_count = $val WHERE ID = 1");
        return (int)$val;
    });

    // ---- wp_set_comment_status ----
    $results[] = run_test($conn, "wp_set_comment_status()", "COMMENTS", function($c) {
        q($c, "UPDATE wp_comments SET comment_approved = '0' WHERE comment_ID = 2");
        return 1;
    });

    // ---- wp_delete_comment ----
    $results[] = run_test($conn, "wp_delete_comment()", "COMMENTS", function($c) {
        q($c, "DELETE FROM wp_commentmeta WHERE comment_id = 21");
        q($c, "DELETE FROM wp_comments WHERE comment_ID = 21");
        return 1;
    });

    // ---- Comment feed query (WP_Comment_Query) ----
    $results[] = run_test($conn, "Recent approved comments", "COMMENTS", function($c) {
        return q_count($c, "SELECT wp_comments.comment_ID FROM wp_comments JOIN wp_posts ON (wp_comments.comment_post_ID = wp_posts.ID) WHERE comment_approved = '1' AND post_status = 'publish' ORDER BY wp_comments.comment_date_gmt DESC LIMIT 0, 10");
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 9: TAXONOMY QUERIES
//
//  Exact queries from wp-includes/taxonomy.php:
//    wp_insert_term(), wp_delete_term(), wp_update_term(),
//    wp_set_object_terms(), wp_get_object_terms()
//
// ===========================================================================

function test_taxonomy_queries(mysqli $conn): array {
    $results = [];

    // ---- wp_insert_term: add categories and tags ----
    $results[] = run_test($conn, "wp_insert_term() categories", "TAXONOMY", function($c) {
        $categories = [
            [2, 'Technology', 'technology'],
            [3, 'Science', 'science'],
            [4, 'Lifestyle', 'lifestyle'],
            [5, 'News', 'news'],
        ];
        $count = 0;
        foreach ($categories as [$id, $name, $slug]) {
            q($c, "INSERT INTO wp_terms (term_id, name, slug, term_group) VALUES ($id, '$name', '$slug', 0)");
            q($c, "INSERT INTO wp_term_taxonomy (term_id, taxonomy, description, parent, count) VALUES ($id, 'category', '', 0, 0)");
            $count++;
        }
        return $count;
    });

    $results[] = run_test($conn, "wp_insert_term() tags", "TAXONOMY", function($c) {
        $tags = [
            [6, 'php', 'php'],
            [7, 'mysql', 'mysql'],
            [8, 'wordpress', 'wordpress'],
            [9, 'performance', 'performance'],
            [10, 'security', 'security'],
        ];
        $count = 0;
        foreach ($tags as [$id, $name, $slug]) {
            q($c, "INSERT INTO wp_terms (term_id, name, slug, term_group) VALUES ($id, '$name', '$slug', 0)");
            q($c, "INSERT INTO wp_term_taxonomy (term_id, taxonomy, description, parent, count) VALUES ($id, 'post_tag', '', 0, 0)");
            $count++;
        }
        return $count;
    });

    // ---- Duplicate term check (wp_insert_term) ----
    $results[] = run_test($conn, "Check duplicate term slug", "TAXONOMY", function($c) {
        return q_count($c, "SELECT t.term_id, t.slug, tt.term_taxonomy_id FROM wp_terms AS t INNER JOIN wp_term_taxonomy AS tt ON t.term_id = tt.term_id WHERE t.slug = 'technology' AND tt.taxonomy = 'category'");
    });

    // ---- wp_set_object_terms: assign terms to posts ----
    $results[] = run_test($conn, "wp_set_object_terms() assign categories", "TAXONOMY", function($c) {
        $count = 0;
        // Assign posts to categories via term_relationships
        for ($post_id = 5; $post_id <= 20; $post_id++) {
            $tt_id = ($post_id % 5) + 1; // category term_taxonomy_ids 1-5
            q($c, "INSERT INTO wp_term_relationships (object_id, term_taxonomy_id, term_order) VALUES ($post_id, $tt_id, 0)");
            $count++;
        }
        return $count;
    });

    $results[] = run_test($conn, "wp_set_object_terms() assign tags", "TAXONOMY", function($c) {
        $count = 0;
        for ($post_id = 5; $post_id <= 20; $post_id++) {
            $tt_id = ($post_id % 5) + 6; // tag term_taxonomy_ids 6-10
            q($c, "INSERT INTO wp_term_relationships (object_id, term_taxonomy_id, term_order) VALUES ($post_id, $tt_id, 0)");
            $count++;
        }
        return $count;
    });

    // ---- wp_get_object_terms: get terms for a post ----
    $results[] = run_test($conn, "wp_get_object_terms() for post", "TAXONOMY", function($c) {
        return q_count($c, "SELECT t.*, tt.* FROM wp_terms AS t INNER JOIN wp_term_taxonomy AS tt ON t.term_id = tt.term_id INNER JOIN wp_term_relationships AS tr ON tr.term_taxonomy_id = tt.term_taxonomy_id WHERE tt.taxonomy IN ('category', 'post_tag') AND tr.object_id IN (5) ORDER BY t.name ASC");
    });

    // ---- get_terms: list all categories ----
    $results[] = run_test($conn, "get_terms() all categories", "TAXONOMY", function($c) {
        return q_count($c, "SELECT t.*, tt.* FROM wp_terms AS t INNER JOIN wp_term_taxonomy AS tt ON t.term_id = tt.term_id WHERE tt.taxonomy IN ('category') ORDER BY t.name ASC");
    });

    // ---- get_terms: list all tags ----
    $results[] = run_test($conn, "get_terms() all tags", "TAXONOMY", function($c) {
        return q_count($c, "SELECT t.*, tt.* FROM wp_terms AS t INNER JOIN wp_term_taxonomy AS tt ON t.term_id = tt.term_id WHERE tt.taxonomy IN ('post_tag') ORDER BY t.name ASC");
    });

    // ---- Term count update (after assigning terms) ----
    $results[] = run_test($conn, "Update term counts", "TAXONOMY", function($c) {
        q($c, "SELECT term_taxonomy_id FROM wp_term_taxonomy WHERE taxonomy = 'category'");
        // WordPress updates each term_taxonomy count based on published posts
        q($c, "UPDATE wp_term_taxonomy AS tt SET count = (SELECT COUNT(*) FROM wp_term_relationships AS tr INNER JOIN wp_posts ON wp_posts.ID = tr.object_id WHERE tr.term_taxonomy_id = tt.term_taxonomy_id AND wp_posts.post_status = 'publish' AND wp_posts.post_type = 'post') WHERE tt.taxonomy = 'category'");
        q($c, "UPDATE wp_term_taxonomy AS tt SET count = (SELECT COUNT(*) FROM wp_term_relationships AS tr INNER JOIN wp_posts ON wp_posts.ID = tr.object_id WHERE tr.term_taxonomy_id = tt.term_taxonomy_id AND wp_posts.post_status = 'publish' AND wp_posts.post_type = 'post') WHERE tt.taxonomy = 'post_tag'");
        return 1;
    });

    // ---- Get objects in term ----
    $results[] = run_test($conn, "get_objects_in_term()", "TAXONOMY", function($c) {
        return q_count($c, "SELECT tr.object_id FROM wp_term_relationships AS tr INNER JOIN wp_term_taxonomy AS tt ON tr.term_taxonomy_id = tt.term_taxonomy_id WHERE tt.taxonomy IN ('category') AND tt.term_id IN (2)");
    });

    // ---- Delete term relationship ----
    $results[] = run_test($conn, "wp_remove_object_terms()", "TAXONOMY", function($c) {
        q($c, "DELETE FROM wp_term_relationships WHERE object_id = 5 AND term_taxonomy_id = 6");
        return 1;
    });

    // ---- wp_delete_term ----
    $results[] = run_test($conn, "wp_delete_term()", "TAXONOMY", function($c) {
        // Get relationships first
        q($c, "SELECT object_id FROM wp_term_relationships WHERE term_taxonomy_id = 10");
        // Delete relationships
        q($c, "DELETE FROM wp_term_relationships WHERE term_taxonomy_id = 10");
        // Delete term_taxonomy
        q($c, "DELETE FROM wp_term_taxonomy WHERE term_taxonomy_id = 10");
        // Check if term is used in any other taxonomy
        $remaining = q_val($c, "SELECT COUNT(*) FROM wp_term_taxonomy WHERE term_id = 10");
        if ((int)$remaining === 0) {
            // Delete termmeta
            q($c, "SELECT meta_id FROM wp_termmeta WHERE term_id = 10");
            q($c, "DELETE FROM wp_termmeta WHERE term_id = 10");
            // Delete term itself
            q($c, "DELETE FROM wp_terms WHERE term_id = 10");
        }
        return 1;
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 10: ADMIN / UPGRADE QUERIES
//
//  Miscellaneous queries from upgrade.php and admin operations
//
// ===========================================================================

function test_admin_queries(mysqli $conn): array {
    $results = [];

    // ---- SHOW TABLES (maybe_create_table check) ----
    $results[] = run_test($conn, "SHOW TABLES LIKE 'wp_posts'", "ADMIN", function($c) {
        return q_count($c, "SHOW TABLES LIKE 'wp_posts'");
    });

    // ---- SHOW INDEX (dbDelta uses this) ----
    $results[] = run_test($conn, "SHOW INDEX FROM wp_posts", "ADMIN", function($c) {
        return q_count($c, "SHOW INDEX FROM wp_posts");
    });

    $results[] = run_test($conn, "SHOW INDEX FROM wp_options", "ADMIN", function($c) {
        return q_count($c, "SHOW INDEX FROM wp_options");
    });

    // ---- DESCRIBE (maybe_add_column uses this) ----
    $results[] = run_test($conn, "DESCRIBE wp_options", "ADMIN", function($c) {
        return q_count($c, "DESCRIBE wp_options");
    });

    $results[] = run_test($conn, "DESCRIBE wp_users", "ADMIN", function($c) {
        return q_count($c, "DESCRIBE wp_users");
    });

    // ---- Duplicate option check (upgrade_130 pattern) ----
    $results[] = run_test($conn, "Find duplicate options", "ADMIN", function($c) {
        return q_count($c, "SELECT option_name, COUNT(option_name) AS dupes FROM wp_options GROUP BY option_name HAVING dupes > 1");
    });

    // ---- Comment count rebuild (upgrade_160) ----
    $results[] = run_test($conn, "Rebuild comment counts", "ADMIN", function($c) {
        return q_count($c, "SELECT comment_post_ID, COUNT(*) as c FROM wp_comments WHERE comment_approved = '1' GROUP BY comment_post_ID");
    });

    // ---- Post slug generation (upgrade_100 checks empty slugs) ----
    $results[] = run_test($conn, "Find posts with empty slug", "ADMIN", function($c) {
        return q_count($c, "SELECT ID, post_title, post_name FROM wp_posts WHERE post_name = ''");
    });

    // ---- Subquery: posts by type+status counts (dashboard widget) ----
    $results[] = run_test($conn, "Dashboard post counts", "ADMIN", function($c) {
        return q_count($c, "SELECT post_status, COUNT(*) AS num_posts FROM wp_posts WHERE post_type = 'post' GROUP BY post_status");
    });

    // ---- Subquery: page counts ----
    $results[] = run_test($conn, "Dashboard page counts", "ADMIN", function($c) {
        return q_count($c, "SELECT post_status, COUNT(*) AS num_posts FROM wp_posts WHERE post_type = 'page' GROUP BY post_status");
    });

    // ---- REPLACE INTO (WordPress uses this for some operations) ----
    $results[] = run_test($conn, "REPLACE INTO wp_options", "ADMIN", function($c) {
        q($c, "REPLACE INTO wp_options (option_name, option_value, autoload) VALUES ('db_version', '57155', 'yes')");
        return 1;
    });

    // ---- Check link existence (upgrade_350) ----
    $results[] = run_test($conn, "SELECT link_id LIMIT 1", "ADMIN", function($c) {
        return q_count($c, "SELECT link_id FROM wp_links LIMIT 1");
    });

    // ---- Batch meta cleanup (upgrade_300 pattern) ----
    $results[] = run_test($conn, "DELETE orphaned usermeta", "ADMIN", function($c) {
        q($c, "DELETE FROM wp_usermeta WHERE meta_key = '_nonexistent_capability_key' AND user_id NOT IN (SELECT ID FROM wp_users)");
        return 0;
    });

    return $results;
}


// ===========================================================================
//
//  SECTION 11: COMPLEX / REAL-WORLD WORDPRESS QUERY PATTERNS
//
//  Multi-step operations WordPress performs in typical pageloads
//
// ===========================================================================

function test_realworld_patterns(mysqli $conn): array {
    $results = [];

    // ---- Front page load: load all autoloaded options + recent posts ----
    $results[] = run_test($conn, "Front page: autoload options", "PAGELOAD", function($c) {
        return q_count($c, "SELECT option_name, option_value FROM wp_options WHERE autoload IN ('yes', 'on', 'auto-on')");
    });

    $results[] = run_test($conn, "Front page: recent posts", "PAGELOAD", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND wp_posts.post_type = 'post' AND (wp_posts.post_status = 'publish') ORDER BY wp_posts.post_date DESC LIMIT 0, 10");
    });

    $results[] = run_test($conn, "Front page: post meta cache", "PAGELOAD", function($c) {
        return q_count($c, "SELECT post_id, meta_key, meta_value FROM wp_postmeta WHERE post_id IN (1, 2, 3, 5, 6, 7, 8, 9, 10, 11) ORDER BY meta_id ASC");
    });

    $results[] = run_test($conn, "Front page: term cache", "PAGELOAD", function($c) {
        return q_count($c, "SELECT t.*, tt.* FROM wp_terms AS t INNER JOIN wp_term_taxonomy AS tt ON t.term_id = tt.term_id INNER JOIN wp_term_relationships AS tr ON tr.term_taxonomy_id = tt.term_taxonomy_id WHERE tr.object_id IN (1, 5, 6, 7, 8, 9, 10, 11) ORDER BY t.name ASC");
    });

    // ---- Single post view ----
    $results[] = run_test($conn, "Single post: get by slug", "PAGELOAD", function($c) {
        return q_count($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND wp_posts.post_name = 'hello-world' AND wp_posts.post_type = 'post' LIMIT 1");
    });

    $results[] = run_test($conn, "Single post: get comments", "PAGELOAD", function($c) {
        return q_count($c, "SELECT wp_comments.comment_ID FROM wp_comments WHERE comment_post_ID = 1 AND comment_approved = '1' AND comment_type IN ('comment', '') ORDER BY wp_comments.comment_date_gmt ASC");
    });

    $results[] = run_test($conn, "Single post: adjacent posts", "PAGELOAD", function($c) {
        q($c, "SELECT p.ID FROM wp_posts AS p WHERE p.post_date < '2024-01-01 00:00:00' AND p.post_type = 'post' AND p.post_status = 'publish' ORDER BY p.post_date DESC LIMIT 1");
        q($c, "SELECT p.ID FROM wp_posts AS p WHERE p.post_date > '2024-01-01 00:00:00' AND p.post_type = 'post' AND p.post_status = 'publish' ORDER BY p.post_date ASC LIMIT 1");
        return 2;
    });

    // ---- Admin: edit posts list ----
    $results[] = run_test($conn, "Admin: posts list", "PAGELOAD", function($c) {
        $count = q_val($c, "SELECT COUNT(*) FROM wp_posts WHERE 1=1 AND wp_posts.post_type = 'post' AND (wp_posts.post_status = 'publish' OR wp_posts.post_status = 'future' OR wp_posts.post_status = 'draft' OR wp_posts.post_status = 'pending' OR wp_posts.post_status = 'private')");
        q($c, "SELECT wp_posts.* FROM wp_posts WHERE 1=1 AND wp_posts.post_type = 'post' AND (wp_posts.post_status = 'publish' OR wp_posts.post_status = 'future' OR wp_posts.post_status = 'draft' OR wp_posts.post_status = 'pending' OR wp_posts.post_status = 'private') ORDER BY wp_posts.post_date DESC LIMIT 0, 20");
        return (int)$count;
    });

    // ---- Admin: dashboard recent comments ----
    $results[] = run_test($conn, "Admin: recent comments", "PAGELOAD", function($c) {
        return q_count($c, "SELECT wp_comments.* FROM wp_comments INNER JOIN wp_posts ON wp_comments.comment_post_ID = wp_posts.ID WHERE wp_posts.post_status != 'trash' ORDER BY wp_comments.comment_date_gmt DESC LIMIT 0, 5");
    });

    // ---- wp-login.php: authenticate user ----
    $results[] = run_test($conn, "Login: authenticate", "PAGELOAD", function($c) {
        // 1. Look up user by login
        q($c, "SELECT * FROM wp_users WHERE user_login = 'admin'");
        // 2. Load user meta (capabilities, etc.)
        q($c, "SELECT user_id, meta_key, meta_value FROM wp_usermeta WHERE user_id IN (1) ORDER BY umeta_id ASC");
        // 3. Update session token
        q($c, "UPDATE wp_usermeta SET meta_value = 'a:1:{s:64:\"newtokenhash\";a:4:{s:10:\"expiration\";i:1700100000;s:2:\"ip\";s:9:\"127.0.0.1\";s:2:\"ua\";s:0:\"\";s:5:\"login\";i:1700000000;}}' WHERE user_id = 1 AND meta_key = 'session_tokens'");
        return 3;
    });

    // ---- AJAX: heartbeat (WordPress sends this every 15-60 seconds) ----
    $results[] = run_test($conn, "Heartbeat: check post lock", "PAGELOAD", function($c) {
        $val = q_val($c, "SELECT meta_value FROM wp_postmeta WHERE post_id = 1 AND meta_key = '_edit_lock'");
        return $val ? 1 : 0;
    });

    // ---- Cron: check scheduled events ----
    $results[] = run_test($conn, "Cron: get cron option", "PAGELOAD", function($c) {
        return q_count($c, "SELECT option_value FROM wp_options WHERE option_name = 'cron' LIMIT 1");
    });

    return $results;
}


// ===========================================================================
//  Runner + Output
// ===========================================================================

function run_all_tests(mysqli $conn, string $engine): array {
    $all = [];
    $all = array_merge($all, test_connection_init($conn));
    $all = array_merge($all, test_schema_creation($conn));
    $all = array_merge($all, test_install_data($conn));
    $all = array_merge($all, test_options_queries($conn));
    $all = array_merge($all, test_post_queries($conn));
    $all = array_merge($all, test_meta_queries($conn));
    $all = array_merge($all, test_user_queries($conn));
    $all = array_merge($all, test_comment_queries($conn));
    $all = array_merge($all, test_taxonomy_queries($conn));
    $all = array_merge($all, test_admin_queries($conn));
    $all = array_merge($all, test_realworld_patterns($conn));
    return $all;
}

function print_results(string $engine, array $results): void {
    $categories = [];
    foreach ($results as $r) {
        $categories[$r->category][] = $r;
    }

    $total_ms = 0;
    $passed = 0;
    $failed = 0;

    echo "\n";
    echo str_repeat("=", 95) . "\n";
    echo "  $engine - WordPress SQL Compatibility Test Results\n";
    echo str_repeat("=", 95) . "\n";

    foreach ($categories as $cat => $cat_results) {
        echo "\n  [$cat]\n";
        echo sprintf("  %-55s %10s %8s %s\n", "Test", "Time (ms)", "Rows", "Status");
        echo "  " . str_repeat("-", 91) . "\n";

        foreach ($cat_results as $r) {
            $status = $r->success ? "  OK" : "FAIL";
            $time_str = sprintf("%10.2f", $r->duration_ms);
            $rows_str = sprintf("%8d", $r->rows_affected);
            $line = sprintf("  %-55s %s %s %s", $r->name, $time_str, $rows_str, $status);

            if (!$r->success) {
                echo $line . "\n";
                // Print error on next line, wrapping at 89 chars
                $err = $r->error;
                while (strlen($err) > 0) {
                    echo "    ERR: " . substr($err, 0, 80) . "\n";
                    $err = substr($err, 80);
                }
                $failed++;
            } else {
                echo $line . "\n";
                $passed++;
            }

            $total_ms += $r->duration_ms;
        }
    }

    echo "\n" . str_repeat("-", 95) . "\n";
    echo sprintf("  %-55s %10.2f\n", "TOTAL TIME", $total_ms);
    echo sprintf("  Passed: %d  |  Failed: %d  |  Total: %d\n", $passed, $failed, count($results));
    echo str_repeat("=", 95) . "\n";
}

function print_comparison(array $forge_results, array $mysql_results): void {
    echo "\n";
    echo str_repeat("=", 100) . "\n";
    echo "  Comparison: ForgeDB vs MySQL - WordPress SQL Compatibility\n";
    echo str_repeat("=", 100) . "\n";
    echo sprintf("  %-50s %12s %12s %10s\n", "Test", "ForgeDB(ms)", "MySQL(ms)", "Ratio");
    echo str_repeat("-", 100) . "\n";

    $forge_total = 0;
    $mysql_total = 0;
    $both_pass = 0;
    $forge_only_fail = 0;

    for ($i = 0; $i < count($forge_results); $i++) {
        $f = $forge_results[$i];
        $m = $mysql_results[$i] ?? null;

        if (!$f->success && $m && $m->success) {
            $ratio = "FORGE-FAIL";
            $forge_only_fail++;
        } elseif (!$f->success || !$m || !$m->success) {
            $ratio = "N/A";
        } else {
            $r = $m->duration_ms > 0 ? $f->duration_ms / $m->duration_ms : 0;
            $ratio = sprintf("%.2fx", $r);
            $both_pass++;
        }

        $ft = sprintf("%12.2f", $f->duration_ms);
        $mt = $m ? sprintf("%12.2f", $m->duration_ms) : "       N/A";

        echo sprintf("  %-50s %s %s %10s\n", $f->name, $ft, $mt, $ratio);

        $forge_total += $f->duration_ms;
        if ($m) $mysql_total += $m->duration_ms;
    }

    echo str_repeat("-", 100) . "\n";
    $total_ratio = $mysql_total > 0 ? sprintf("%.2fx", $forge_total / $mysql_total) : "N/A";
    echo sprintf("  %-50s %12.2f %12.2f %10s\n", "TOTAL", $forge_total, $mysql_total, $total_ratio);
    echo "\n  Both passed: $both_pass | ForgeDB-only failures: $forge_only_fail\n";
    echo "  Ratio < 1.0x = ForgeDB faster | Ratio > 1.0x = MySQL faster\n";
    echo str_repeat("=", 100) . "\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

echo "\n  WordPress SQL Compatibility Test Suite\n";
echo "  ======================================\n\n";

$forge_results = null;
$mysql_results = null;

if ($mode === 'forgedb' || $mode === 'both') {
    echo "  Connecting to ForgeDB ($FORGEDB_HOST:$FORGEDB_PORT)...\n";
    mysqli_report(MYSQLI_REPORT_OFF);
    $conn = @new mysqli($FORGEDB_HOST, $FORGEDB_USER, $FORGEDB_PASS, '', $FORGEDB_PORT);
    if ($conn->connect_error) {
        echo "  SKIP: Cannot connect to ForgeDB on port $FORGEDB_PORT\n";
        echo "  Start it: cargo run --release --bin forgedb-server -- ./bench_data 0.0.0.0:3307\n";
    } else {
        echo "  Connected! Running all WordPress SQL tests...\n";
        $forge_results = run_all_tests($conn, 'ForgeDB');
        print_results('ForgeDB', $forge_results);
        $conn->close();
    }
}

if ($mode === 'mysql' || $mode === 'both') {
    echo "\n  Connecting to MySQL ($MYSQL_HOST:$MYSQL_PORT)...\n";
    mysqli_report(MYSQLI_REPORT_OFF);
    $conn = @new mysqli($MYSQL_HOST, $MYSQL_USER, $MYSQL_PASS, '', $MYSQL_PORT);
    if ($conn->connect_error) {
        echo "  SKIP: Cannot connect to MySQL on port $MYSQL_PORT\n";
    } else {
        echo "  Connected! Setting up database...\n";
        $conn->query("CREATE DATABASE IF NOT EXISTS $MYSQL_DB");
        $conn->select_db($MYSQL_DB);
        echo "  Running all WordPress SQL tests...\n";
        $mysql_results = run_all_tests($conn, 'MySQL');
        print_results('MySQL', $mysql_results);
        $conn->close();
    }
}

if ($forge_results && $mysql_results) {
    print_comparison($forge_results, $mysql_results);
}

echo "\n  Done.\n\n";
