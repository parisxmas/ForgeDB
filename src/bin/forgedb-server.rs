use forgedb::server::MysqlServer;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args.get(1).map(|s| s.as_str()).unwrap_or("./forgedb_data");
    let bind_addr = args.get(2).map(|s| s.as_str()).unwrap_or("0.0.0.0:3307");

    println!("ForgeDB MySQL-compatible server");
    println!("Database path: {}", db_path);
    println!("Listening on: {}", bind_addr);

    let server = MysqlServer::new(db_path, bind_addr);
    if let Err(e) = server.start() {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
