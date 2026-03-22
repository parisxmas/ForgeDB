use forgedb::server::{MysqlServer, TdsServer, ForgeWireServer};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args.get(1).map(|s| s.as_str()).unwrap_or("./forgedb_data");
    let mysql_addr = args.get(2).map(|s| s.as_str()).unwrap_or("0.0.0.0:3307");
    let tds_addr = args.get(3).map(|s| s.as_str()).unwrap_or("0.0.0.0:1433");
    let forge_addr = args.get(4).map(|s| s.as_str()).unwrap_or("0.0.0.0:5433");

    println!("ForgeDB server");
    println!("Database path:  {}", db_path);
    println!("MySQL protocol: {}", mysql_addr);
    println!("TDS protocol:   {}", tds_addr);
    println!("ForgeWire:      {}", forge_addr);

    // Start TDS server
    let tds_db = db_path.to_string();
    let tds_bind = tds_addr.to_string();
    std::thread::spawn(move || {
        let server = TdsServer::new(&tds_db, &tds_bind);
        if let Err(e) = server.start() { eprintln!("TDS error: {}", e); }
    });

    // Start ForgeWire server
    let fw_db = db_path.to_string();
    let fw_bind = forge_addr.to_string();
    std::thread::spawn(move || {
        let server = ForgeWireServer::new(&fw_db, &fw_bind);
        if let Err(e) = server.start() { eprintln!("ForgeWire error: {}", e); }
    });

    // MySQL server on main thread
    let server = MysqlServer::new(db_path, mysql_addr);
    if let Err(e) = server.start() {
        eprintln!("MySQL error: {}", e);
        std::process::exit(1);
    }
}
