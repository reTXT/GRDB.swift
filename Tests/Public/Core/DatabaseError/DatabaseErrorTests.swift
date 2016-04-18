import XCTest
#if SQLITE_HAS_CODEC
    import GRDBCipher
#else
    import GRDB
#endif

class DatabaseErrorTests: GRDBTestCase {
    
    func testDatabaseErrorInTransaction() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            do {
                try dbQueue.inTransaction { db in
                    try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY)")
                    try db.execute("CREATE TABLE pets (masterId INTEGER NOT NULL REFERENCES persons(id), name TEXT)")
                    self.sqlQueries.removeAll()
                    try db.execute("INSERT INTO pets (masterId, name) VALUES (?, ?)", arguments: [1, "Bobby"])
                    return .commit
                }
            } catch let error as DatabaseError {
                XCTAssertEqual(error.code, 19) // SQLITE_CONSTRAINT
                XCTAssertEqual(error.message!, "FOREIGN KEY constraint failed")
                XCTAssertEqual(error.sql!, "INSERT INTO pets (masterId, name) VALUES (?, ?)")
                XCTAssertEqual(error.description, "SQLite error 19 with statement `INSERT INTO pets (masterId, name) VALUES (?, ?)` arguments [1, \"Bobby\"]: FOREIGN KEY constraint failed")
                
                XCTAssertEqual(sqlQueries.count, 2)
                XCTAssertEqual(sqlQueries[0], "INSERT INTO pets (masterId, name) VALUES (1, 'Bobby')")
                XCTAssertEqual(sqlQueries[1], "ROLLBACK TRANSACTION")
            }
        }
    }
    
    func testDatabaseErrorThrownByUpdateStatementContainSQLAndArguments() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE pets (masterId INTEGER NOT NULL REFERENCES persons(id), name TEXT)")
            }
            
            // db.execute(sql, arguments)
            try dbQueue.inDatabase { db in
                do {
                    try db.execute("INSERT INTO pets (masterId, name) VALUES (?, ?)", arguments: [1, "Bobby"])
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.code, 19) // SQLITE_CONSTRAINT
                    XCTAssertEqual(error.message!, "FOREIGN KEY constraint failed")
                    XCTAssertEqual(error.sql!, "INSERT INTO pets (masterId, name) VALUES (?, ?)")
                    XCTAssertEqual(error.description, "SQLite error 19 with statement `INSERT INTO pets (masterId, name) VALUES (?, ?)` arguments [1, \"Bobby\"]: FOREIGN KEY constraint failed")
                }
            }
            
            // statement.execute(arguments)
            try dbQueue.inDatabase { db in
                do {
                    let statement = try db.updateStatement("INSERT INTO pets (masterId, name) VALUES (?, ?)")
                    try statement.execute(arguments: [1, "Bobby"])
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.code, 19) // SQLITE_CONSTRAINT
                    XCTAssertEqual(error.message!, "FOREIGN KEY constraint failed")
                    XCTAssertEqual(error.sql!, "INSERT INTO pets (masterId, name) VALUES (?, ?)")
                    XCTAssertEqual(error.description, "SQLite error 19 with statement `INSERT INTO pets (masterId, name) VALUES (?, ?)` arguments [1, \"Bobby\"]: FOREIGN KEY constraint failed")
                }
            }
            
            // statement.execute()
            try dbQueue.inDatabase { db in
                do {
                    let statement = try db.updateStatement("INSERT INTO pets (masterId, name) VALUES (?, ?)")
                    statement.arguments = [1, "Bobby"]
                    try statement.execute()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.code, 19) // SQLITE_CONSTRAINT
                    XCTAssertEqual(error.message!, "FOREIGN KEY constraint failed")
                    XCTAssertEqual(error.sql!, "INSERT INTO pets (masterId, name) VALUES (?, ?)")
                    XCTAssertEqual(error.description, "SQLite error 19 with statement `INSERT INTO pets (masterId, name) VALUES (?, ?)` arguments [1, \"Bobby\"]: FOREIGN KEY constraint failed")
                }
            }
        }
    }
    
    func testDatabaseErrorThrownByExecuteMultiStatementContainSQL() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                do {
                    try db.execute(
                        "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INT);" +
                            "CREATE TABLE pets (masterId INTEGER NOT NULL REFERENCES persons(id), name TEXT);" +
                        "INSERT INTO pets (masterId, name) VALUES (1, 'Bobby')")
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.code, 19) // SQLITE_CONSTRAINT
                    XCTAssertEqual(error.message!, "FOREIGN KEY constraint failed")
                    XCTAssertEqual(error.sql!, "INSERT INTO pets (masterId, name) VALUES (1, 'Bobby')")
                    XCTAssertEqual(error.description, "SQLite error 19 with statement `INSERT INTO pets (masterId, name) VALUES (1, 'Bobby')`: FOREIGN KEY constraint failed")
                }
            }
        }
    }

}
