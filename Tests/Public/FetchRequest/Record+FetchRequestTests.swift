import XCTest
#if SQLITE_HAS_CODEC
    import GRDBCipher
#else
    import GRDB
#endif

private class Reader : Record {
    var id: Int64?
    let name: String
    let age: Int?
    
    init(id: Int64?, name: String, age: Int?) {
        self.id = id
        self.name = name
        self.age = age
        super.init()
    }
    
    required init(row: Row) {
        self.id = row.value(named: "id")
        self.name = row.value(named: "name")
        self.age = row.value(named: "age")
        super.init(row: row)
    }
    
    override static func databaseTableName() -> String {
        return "readers"
    }
    
    override var persistentDictionary: [String: DatabaseValueConvertible?] {
        return ["id": id, "name": name, "age": age]
    }
    
    override func didInsertWithRowID(rowID: Int64, forColumn column: String?) {
        id = rowID
    }
}


class RecordFetchRequestTests: GRDBTestCase {
    
    override func setUpDatabase(dbWriter: DatabaseWriter) throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("createReaders") { db in
            try db.execute(
                "CREATE TABLE readers (" +
                    "id INTEGER PRIMARY KEY, " +
                    "name TEXT NOT NULL, " +
                    "age INT" +
                ")")
        }
        try migrator.migrate(dbWriter)
    }
    
    
    // MARK: - Fetch Record
    
    func testFetch() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                let arthur = Reader(id: nil, name: "Arthur", age: 42)
                try arthur.insert(db)
                let barbara = Reader(id: nil, name: "Barbara", age: 36)
                try barbara.insert(db)
                
                let request = Reader.all()
                
                do {
                    let readers = request.fetchAll(db)
                    XCTAssertEqual(self.lastSQLQuery, "SELECT * FROM \"readers\"")
                    XCTAssertEqual(readers.count, 2)
                    XCTAssertEqual(readers[0].id!, arthur.id!)
                    XCTAssertEqual(readers[0].name, arthur.name)
                    XCTAssertEqual(readers[0].age, arthur.age)
                    XCTAssertEqual(readers[1].id!, barbara.id!)
                    XCTAssertEqual(readers[1].name, barbara.name)
                    XCTAssertEqual(readers[1].age, barbara.age)
                }
                
                do {
                    let reader = request.fetchOne(db)!
                    XCTAssertEqual(self.lastSQLQuery, "SELECT * FROM \"readers\"")
                    XCTAssertEqual(reader.id!, arthur.id!)
                    XCTAssertEqual(reader.name, arthur.name)
                    XCTAssertEqual(reader.age, arthur.age)
                }
                
                do {
                    let names = request.fetch(db).map { $0.name }
                    XCTAssertEqual(self.lastSQLQuery, "SELECT * FROM \"readers\"")
                    XCTAssertEqual(names, [arthur.name, barbara.name])
                }
            }
        }
    }
}
