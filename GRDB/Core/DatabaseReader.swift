/// The protocol for all types that can fetch values from a database.
///
/// It is adopted by DatabaseQueue and DatabasePool.
///
/// The protocol comes with isolation guarantees that describe the behavior of
/// adopting types in a multithreaded application.
///
/// Types that adopt the protocol can provide in practice stronger guarantees.
/// For example, DatabaseQueue provides a stronger isolation level
/// than DatabasePool.
///
/// **Warning**: Isolation guarantees stand as long as there is no external
/// connection to the database. Should you have to cope with external
/// connections, protect yourself with transactions, and be ready to setup a
/// [busy handler](https://www.sqlite.org/c3ref/busy_handler.html).
public protocol DatabaseReader : class {
    
    // MARK: - Read From Database
    
    /// Synchronously executes a read-only block that takes a database
    /// connection, and returns its result.
    ///
    /// The *block* argument is completely isolated. Eventual concurrent
    /// database updates are *not visible* inside the block:
    ///
    ///     reader.read { db in
    ///         // Those two values are guaranteed to be equal, even if the
    ///         // `wines` table is modified between the two requests:
    ///         let count1 = Int.fetchOne(db, "SELECT COUNT(*) FROM wines")!
    ///         let count2 = Int.fetchOne(db, "SELECT COUNT(*) FROM wines")!
    ///     }
    ///
    ///     reader.read { db in
    ///         // Now this value may be different:
    ///         let count = Int.fetchOne(db, "SELECT COUNT(*) FROM wines")!
    ///     }
    ///
    /// - parameter block: A block that accesses the database.
    /// - throws: The error thrown by the block.
    func read<T>(block: (db: Database) throws -> T) rethrows -> T
    
    /// Synchronously executes a read-only block that takes a database
    /// connection, and returns its result.
    ///
    /// Individual statements executed in the *block* argument are executed
    /// in isolation from eventual concurrent updates:
    ///
    ///     reader.nonIsolatedRead { db in
    ///         // no external update can mess with this iteration:
    ///         for row in Row.fetch(db, ...) { ... }
    ///     }
    ///
    /// However, there is no guarantee that consecutive statements have the
    /// same results:
    ///
    ///     reader.nonIsolatedRead { db in
    ///         // Those two ints may be different:
    ///         let sql = "SELECT ..."
    ///         let int1 = Int.fetchOne(db, sql)
    ///         let int2 = Int.fetchOne(db, sql)
    ///     }
    func nonIsolatedRead<T>(block: (db: Database) throws -> T) rethrows -> T
    
    
    // MARK: - Functions
    
    /// Add or redefine an SQL function.
    ///
    ///     let fn = DatabaseFunction("succ", argumentCount: 1) { databaseValues in
    ///         let dbv = databaseValues.first!
    ///         guard let int = dbv.value() as Int? else {
    ///             return nil
    ///         }
    ///         return int + 1
    ///     }
    ///     reader.addFunction(fn)
    ///     Int.fetchOne(reader, "SELECT succ(1)")! // 2
    func addFunction(_ function: DatabaseFunction)
    
    /// Remove an SQL function.
    func removeFunction(_ function: DatabaseFunction)
    
    
    // MARK: - Collations
    
    /// Add or redefine a collation.
    ///
    ///     let collation = DatabaseCollation("localized_standard") { (string1, string2) in
    ///         return (string1 as NSString).localizedStandardCompare(string2)
    ///     }
    ///     reader.addCollation(collation)
    ///     try reader.execute("SELECT * FROM files ORDER BY name COLLATE localized_standard")
    func addCollation(_ collation: DatabaseCollation)
    
    /// Remove a collation.
    func removeCollation(_ collation: DatabaseCollation)
}
