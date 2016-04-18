import XCTest
#if SQLITE_HAS_CODEC
    import GRDBCipher
#else
    import GRDB
#endif

class DatabasePoolCollationTests: GRDBTestCase {
    
    func testCollationIsSharedBetweenWriterAndReaders() {
        assertNoError {
            let dbPool = try makeDatabasePool()
            
            let collation1 = DatabaseCollation(name: "collation1") { (string1, string2) in
                return (string1 == string2) ? .OrderedSame : ((string1 < string2) ? .OrderedAscending : .OrderedDescending)
            }
            dbPool.addCollation(collation1)
            
            try dbPool.write { db in
                try db.execute("CREATE TABLE items (text TEXT COLLATE collation1)")
                try db.execute("INSERT INTO items (text) VALUES ('a')")
                try db.execute("INSERT INTO items (text) VALUES ('b')")
                try db.execute("INSERT INTO items (text) VALUES ('c')")
            }
            dbPool.read { db in
                XCTAssertEqual(String.fetchAll(db, "SELECT text FROM items ORDER BY text"), ["a", "b", "c"])
                XCTAssertEqual(String.fetchAll(db, "SELECT text FROM items ORDER BY text COLLATE collation1"), ["a", "b", "c"])
            }
            
            let collation2 = DatabaseCollation(name: "collation2") { (string1, string2) in
                return (string1 == string2) ? .OrderedSame : ((string1 < string2) ? .OrderedDescending : .OrderedAscending)
            }
            dbPool.addCollation(collation2)
            
            dbPool.read { db in
                XCTAssertEqual(String.fetchAll(db, "SELECT text FROM items ORDER BY text COLLATE collation2"), ["c", "b", "a"])
            }
        }
    }
}
