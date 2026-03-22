use forgedb::server::{MysqlServer, TdsServer};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args.get(1).map(|s| s.as_str()).unwrap_or("./forgedb_data");
    let mysql_addr = args.get(2).map(|s| s.as_str()).unwrap_or("0.0.0.0:3307");
    let tds_addr = args.get(3).map(|s| s.as_str()).unwrap_or("0.0.0.0:1433");

    println!("ForgeDB server");
    println!("Database path: {}", db_path);
    println!("MySQL protocol: {}", mysql_addr);
    println!("TDS protocol:   {}", tds_addr);

    // Start TDS server on a separate thread
    let tds_db_path = db_path.to_string();
    let tds_bind = tds_addr.to_string();
    std::thread::spawn(move || {
        let server = TdsServer::new(&tds_db_path, &tds_bind);
        if let Err(e) = server.start() {
            eprintln!("TDS server error: {}", e);
        }
    });

    // MySQL server on main thread
    let server = MysqlServer::new(db_path, mysql_addr);
    if let Err(e) = server.start() {
        eprintln!("MySQL server error: {}", e);
        std::process::exit(1);
    }
}
