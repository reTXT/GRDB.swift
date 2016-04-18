import XCTest
#if SQLITE_HAS_CODEC
    import GRDBCipher
#else
    import GRDB
#endif

class EventRecorder : Record {
    var id: Int64?
    var awakeFromFetchCount = 0
    
    init(id: Int64? = nil) {
        self.id = id
        super.init()
    }
    
    static func setupInDatabase(db: Database) throws {
        try db.execute("CREATE TABLE eventRecorders (id INTEGER PRIMARY KEY)")
    }
    
    // Record
    
    override static func databaseTableName() -> String {
        return "eventRecorders"
    }
    
    required init(row: Row) {
        id = row.value(named: "id")
        super.init(row: row)
    }
    
    override var persistentDictionary: [String: DatabaseValueConvertible?] {
        return ["id": id]
    }
    
    override func didInsert(with rowID: Int64, for column: String?) {
        self.id = rowID
    }
    
    override func awakeFromFetch(row row: Row) {
        super.awakeFromFetch(row: row)
        awakeFromFetchCount += 1
    }
}

class RecordEventsTests: GRDBTestCase {
    
    override func setUpDatabase(dbWriter: DatabaseWriter) throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("createEventRecorder", migrate: EventRecorder.setupInDatabase)
        try migrator.migrate(dbWriter)
    }
    
    func testAwakeFromFetchIsNotTriggeredByInit() {
        let record = EventRecorder()
        XCTAssertEqual(record.awakeFromFetchCount, 0)
    }
    
    func testAwakeFromFetchIsNotTriggeredByInitFromRow() {
        let record = EventRecorder(Row())
        XCTAssertEqual(record.awakeFromFetchCount, 0)
    }
    
    func testAwakeFromFetchIsTriggeredByFetch() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                do {
                    let record = EventRecorder()
                    try record.insert(db)
                    XCTAssertEqual(record.awakeFromFetchCount, 0)
                }
            }
        }
    }
    func testAwakeFromFetchIsTriggeredByReload() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try EventRecorder().insert(db)
                do {
                    let record = EventRecorder.fetchOne(db, "SELECT * FROM eventRecorders")!
                    XCTAssertEqual(record.awakeFromFetchCount, 1)
                }
            }
        }
    }
}
