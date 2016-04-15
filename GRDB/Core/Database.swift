import Foundation

#if !SQLITE_HAS_CODEC
    #if os(OSX)
        import SQLiteMacOSX
    #elseif os(iOS)
        #if (arch(i386) || arch(x86_64))
            import SQLiteiPhoneSimulator
        #else
            import SQLiteiPhoneOS
        #endif
    #endif
#endif

/// A raw SQLite connection, suitable for the SQLite C API.
public typealias SQLiteConnection = OpaquePointer

/// A raw SQLite function argument.
typealias SQLiteValue = OpaquePointer


/// A Database connection.
///
/// You don't create a database directly. Instead, you use a DatabaseQueue, or
/// a DatabasePool:
///
///     let dbQueue = DatabaseQueue(...)
///
///     // The Database is the `db` in the closure:
///     dbQueue.inDatabase { db in
///         db.execute(...)
///     }
public final class Database {
    
    /// The database configuration
    public let configuration: Configuration
    
    /// The raw SQLite connection, suitable for the SQLite C API.
    public let sqliteConnection: SQLiteConnection
    
    var lastErrorMessage: String? { return String(validatingUTF8: sqlite3_errmsg(sqliteConnection)) }
    
    private var functions = Set<DatabaseFunction>()
    private var collations = Set<DatabaseCollation>()
    
    var schemaCache: DatabaseSchemaCache    // @testable
    
    /// See setupTransactionHooks(), updateStatementDidFail(), updateStatementDidExecute()
    private var transactionState: TransactionState = .waitForTransactionCompletion
    
    /// The transaction observers
    private var transactionObservers = [WeakTransactionObserver]()
    
    /// See setupBusyMode()
    private var busyCallback: BusyCallback?
    
    /// The value for the dispatch queue specific that holds the Database identity.
    /// See preconditionValidQueue.
    var dispatchQueueID: UnsafeMutablePointer<Void>? = nil
    
    init(path: String, configuration: Configuration, schemaCache: DatabaseSchemaCache) throws {
        // See https://www.sqlite.org/c3ref/open.html
        var sqliteConnection: SQLiteConnection? = nil
        let code = sqlite3_open_v2(path, &sqliteConnection, configuration.sqliteOpenFlags, nil)
        guard code == SQLITE_OK else {
            throw DatabaseError(code: code, message: String(validatingUTF8: sqlite3_errmsg(sqliteConnection)))
        }
        
        #if SQLITE_HAS_CODEC
            do {
                if let passphrase = configuration.passphrase {
                    try Database.encrypt(sqliteConnection: sqliteConnection!, withPassphrase: passphrase)
                }
                
                // Fail early if key is wrong or missing
                let readCode = sqlite3_exec(sqliteConnection, "SELECT * FROM sqlite_master LIMIT 1", nil, nil, nil)
                guard readCode == SQLITE_OK else {
                    throw DatabaseError(code: readCode, message: String(validatingUTF8: sqlite3_errmsg(sqliteConnection)))
                }
            } catch {
                // deinit is not called: close connection
                sqlite3_close(sqliteConnection)
                throw error
            }
        #endif
        
        self.configuration = configuration
        self.schemaCache = schemaCache
        self.sqliteConnection = sqliteConnection!
        
        configuration.SQLiteConnectionDidOpen?()
        
        // Setup trace first, so that all queries, including initialization queries, are traced.
        setupTrace()
        
        try setupForeignKeys()
        setupBusyMode()
        setupDefaultFunctions()
        setupDefaultCollations()
    }
    
    deinit {
        configuration.SQLiteConnectionDidClose?()
        sqlite3_close(sqliteConnection)
    }
    
    func releaseMemory() {
        sqlite3_db_release_memory(sqliteConnection)
        schemaCache.clear()
    }
    
    private func setupForeignKeys() throws {
        if configuration.foreignKeysEnabled {
            try execute("PRAGMA foreign_keys = ON")
        }
    }
    
    private func setupTrace() {
        guard configuration.trace != nil else {
            return
        }
        let dbPointer = unsafeBitCast(self, to: UnsafeMutablePointer<Void>.self)
        sqlite3_trace(sqliteConnection, { (dbPointer, sql) in
            let database = unsafeBitCast(dbPointer, to: Database.self)
            database.configuration.trace!(String(validatingUTF8: sql)!)
            }, dbPointer)
    }
    
    private func setupBusyMode() {
        switch configuration.busyMode {
        case .immediateError:
            break
            
        case .timeout(let duration):
            let milliseconds = Int32(duration * 1000)
            sqlite3_busy_timeout(sqliteConnection, milliseconds)
            
        case .callback(let callback):
            let dbPointer = unsafeBitCast(self, to: UnsafeMutablePointer<Void>.self)
            busyCallback = callback
            
            sqlite3_busy_handler(
                sqliteConnection,
                { (dbPointer: UnsafeMutablePointer<Void>?, numberOfTries: Int32) in
                    let database = unsafeBitCast(dbPointer, to: Database.self)
                    let callback = database.busyCallback!
                    return callback(numberOfTries: Int(numberOfTries)) ? 1 : 0
                },
                dbPointer)
        }
    }
    
    private func setupDefaultFunctions() {
        // Add support for Swift String functions.
        //
        // Those functions are used by query's interface:
        //
        ///     let nameColumn = SQLColumn("name")
        ///     let request = Person.select(nameColumn.capitalized())
        ///     let names = String.fetchAll(dbQueue, request)   // [String]
        
        add(function: .capitalized)
        add(function: .lowercased)
        add(function: .uppercased)
        
        if #available(iOS 9.0, OSX 10.11, *) {
            add(function: .localizedCapitalized)
            add(function: .localizedLowercase)
            add(function: .localizedUppercase)
        }
    }
    
    private func setupDefaultCollations() {
        // Add support for Swift String comparison functions.
        //
        // Those collations are readily available when creating tables:
        //
        //      let collationName = DatabaseCollation.localizedCaseInsensitiveCompare.name
        //      dbQueue.execute(
        //          "CREATE TABLE persons (" +
        //              "name TEXT COLLATE \(collationName)" +
        //          ")"
        //      )
        
        add(collation: .unicodeCompare)
        add(collation: .caseInsensitiveCompare)
        add(collation: .localizedCaseInsensitiveCompare)
        add(collation: .localizedCompare)
        add(collation: .localizedStandardCompare)
    }
}

/// An SQLite threading mode. See https://www.sqlite.org/threadsafe.html.
enum ThreadingMode {
    case SQLiteDefault
    case multiThread
    case serialized
    
    var sqliteOpenFlags: Int32 {
        switch self {
        case .SQLiteDefault:
            return 0
        case .multiThread:
            return SQLITE_OPEN_NOMUTEX
        case .serialized:
            return SQLITE_OPEN_FULLMUTEX
        }
    }
}


/// See BusyMode and https://www.sqlite.org/c3ref/busy_handler.html
public typealias BusyCallback = (numberOfTries: Int) -> Bool

/// When there are several connections to a database, a connection may try to
/// access the database while it is locked by another connection.
///
/// The BusyMode enum describes the behavior of GRDB when such a situation
/// occurs:
///
/// - .immediateError: The SQLITE_BUSY error is immediately returned to the
///   connection that tries to access the locked database.
///
/// - .timeout: The SQLITE_BUSY error will be returned only if the database
///   remains locked for more than the specified duration.
///
/// - .callback: Perform your custom lock handling.
///
/// To set the busy mode of a database, use Configuration:
///
///     let configuration = Configuration(busyMode: .timeout(1))
///     let dbQueue = DatabaseQueue(path: "...", configuration: configuration)
///
/// Relevant SQLite documentation:
///
/// - https://www.sqlite.org/c3ref/busy_timeout.html
/// - https://www.sqlite.org/c3ref/busy_handler.html
/// - https://www.sqlite.org/lang_transaction.html
/// - https://www.sqlite.org/wal.html
public enum BusyMode {
    /// The SQLITE_BUSY error is immediately returned to the connection that
    /// tries to access the locked database.
    case immediateError
    
    /// The SQLITE_BUSY error will be returned only if the database remains
    /// locked for more than the specified duration.
    case timeout(NSTimeInterval)
    
    /// A custom callback that is called when a database is locked.
    /// See https://www.sqlite.org/c3ref/busy_handler.html
    case callback(BusyCallback)
}


// =========================================================================
// MARK: - SerializedDatabase Support

extension Database {
    
    /// The key for the dispatch queue specific that holds the Database identity.
    /// See preconditionValidQueue.
    static let dispatchQueueIDKey = unsafeBitCast(Database.self, to: UnsafePointer<Void>.self)     // some unique pointer
    
    func preconditionValidQueue(@autoclosure _ message: () -> String = "Database was not used on the correct thread.", file: StaticString = #file, line: UInt = #line) {
        GRDBPrecondition(dispatchQueueID == nil || dispatchQueueID == dispatch_get_specific(Database.dispatchQueueIDKey), message, file: file, line: line)
    }
}


// =========================================================================
// MARK: - Statements

extension Database {
    
    /// Creates a prepared statement that can be reused.
    ///
    ///     let statement = try db.makeSelectStatement("SELECT COUNT(*) FROM persons WHERE age > ?")
    ///     let moreThanTwentyCount = Int.fetchOne(statement, arguments: [20])!
    ///     let moreThanThirtyCount = Int.fetchOne(statement, arguments: [30])!
    ///
    /// - parameter sql: An SQL query.
    /// - returns: A SelectStatement.
    /// - throws: A DatabaseError whenever SQLite could not parse the sql query.
    @warn_unused_result
    public func makeSelectStatement(_ sql: String) throws -> SelectStatement {
        return try SelectStatement(database: self, sql: sql)
    }
    
    @warn_unused_result
    func cachedSelectStatement(_ sql: String) throws -> SelectStatement {
        if let statement = schemaCache.selectStatement(sql) {
            return statement
        }
        
        let statement = try makeSelectStatement(sql)
        schemaCache.setSelectStatement(statement, forSQL: sql)
        return statement
    }
    
    /// Creates a prepared statement that can be reused.
    ///
    ///     let statement = try db.updateStatement("INSERT INTO persons (name) VALUES (?)")
    ///     try statement.execute(arguments: ["Arthur"])
    ///     try statement.execute(arguments: ["Barbara"])
    ///
    /// This method may throw a DatabaseError.
    ///
    /// - parameter sql: An SQL query.
    /// - returns: An UpdateStatement.
    /// - throws: A DatabaseError whenever SQLite could not parse the sql query.
    @warn_unused_result
    public func makeUpdateStatement(_ sql: String) throws -> UpdateStatement {
        return try UpdateStatement(database: self, sql: sql)
    }
    
    @warn_unused_result
    func cachedUpdateStatement(_ sql: String) throws -> UpdateStatement {
        if let statement = schemaCache.updateStatement(sql) {
            return statement
        }
        
        let statement = try makeUpdateStatement(sql)
        schemaCache.setUpdateStatement(statement, forSQL: sql)
        return statement
    }
    
    /// Executes one or several SQL statements, separated by semi-colons.
    ///
    ///     try db.execute(
    ///         "INSERT INTO persons (name) VALUES (:name)",
    ///         arguments: ["name": "Arthur"])
    ///
    ///     try db.execute(
    ///         "INSERT INTO persons (name) VALUES (?);" +
    ///         "INSERT INTO persons (name) VALUES (?);" +
    ///         "INSERT INTO persons (name) VALUES (?);",
    ///         arguments; ['Arthur', 'Barbara', 'Craig'])
    ///
    /// This method may throw a DatabaseError.
    ///
    /// - parameters:
    ///     - sql: An SQL query.
    ///     - arguments: Optional statement arguments.
    /// - returns: A DatabaseChanges.
    /// - throws: A DatabaseError whenever an SQLite error occurs.
    public func execute(_ sql: String, arguments: StatementArguments? = nil) throws -> DatabaseChanges {
        preconditionValidQueue()
        
        // The tricky part is to consume arguments as statements are executed.
        //
        // Here we build two functions:
        // - consumeArguments returns arguments for a statement
        // - validateRemainingArguments validates the remaining arguments, after
        //   all statements have been executed, in the same way
        //   as Statement.validateArguments()
        let consumeArguments: UpdateStatement -> StatementArguments
        let validateRemainingArguments: () throws -> ()
        
        if let arguments = arguments {
            switch arguments.kind {
            case .values(let values):
                // Extract as many values as needed, statement after statement:
                var remainingValues = values
                consumeArguments = { (statement: UpdateStatement) -> StatementArguments in
                    let argumentCount = statement.sqliteArgumentCount
                    defer {
                        if remainingValues.count >= argumentCount {
                            remainingValues = Array(remainingValues.suffix(from: argumentCount))
                        } else {
                            remainingValues = []
                        }
                    }
                    return StatementArguments(remainingValues.prefix(argumentCount))
                }
                // It's not OK if there remains unused arguments:
                validateRemainingArguments = {
                    if !remainingValues.isEmpty {
                        throw DatabaseError(code: SQLITE_MISUSE, message: "wrong number of statement arguments: \(values.count)")
                    }
                }
            case .namedValues:
                // Reuse the dictionary argument for all statements:
                consumeArguments = { _ in return arguments }
                validateRemainingArguments = { _ in }
            }
        } else {
            // Empty arguments for all statements:
            consumeArguments = { _ in return [] }
            validateRemainingArguments = { _ in }
        }
        
        
        // Execute statements
        
        let changedRowsBefore = sqlite3_total_changes(sqliteConnection)
        let sqlCodeUnits = sql.nulTerminatedUTF8
        var error: ErrorProtocol?
        
        // During the execution of sqlite3_prepare_v2, the observer listens to
        // authorization callbacks in order to observe schema changes.
        let observer = StatementCompilationObserver(self)
        observer.start()
        
        sqlCodeUnits.withUnsafeBufferPointer { codeUnits in
            let sqlStart = UnsafePointer<Int8>(codeUnits.baseAddress)!
            let sqlEnd = sqlStart + sqlCodeUnits.count
            var statementStart = sqlStart
            while statementStart < sqlEnd - 1 {
                observer.reset()
                var statementEnd: UnsafePointer<Int8>? = nil
                var sqliteStatement: SQLiteStatement? = nil
                let code = sqlite3_prepare_v2(sqliteConnection, statementStart, -1, &sqliteStatement, &statementEnd)
                guard code == SQLITE_OK else {
                    error = DatabaseError(code: code, message: lastErrorMessage, sql: sql)
                    break
                }
                
                let sqlData = NSData(bytesNoCopy: UnsafeMutablePointer<Void>(statementStart), length: statementEnd! - statementStart, freeWhenDone: false)
                let sql = String(data: sqlData, encoding: NSUTF8StringEncoding)!.trimmingCharacters(in: .whitespacesAndNewlines())
                guard !sql.isEmpty else {
                    break
                }
                
                do {
                    let statement = UpdateStatement(database: self, sql: sql, sqliteStatement: sqliteStatement!, invalidatesDatabaseSchemaCache: observer.invalidatesDatabaseSchemaCache)
                    try statement.execute(arguments: consumeArguments(statement))
                } catch let statementError {
                    error = statementError
                    break
                }
                
                statementStart = statementEnd!
            }
        }
        
        observer.stop()
        
        if let error = error {
            throw error
        }
        
        // Force arguments validity. See UpdateStatement.execute(), and SelectStatement.fetchSequence()
        try! validateRemainingArguments()
        
        let changedRowsAfter = sqlite3_total_changes(sqliteConnection)
        let lastInsertedRowID = sqlite3_last_insert_rowid(sqliteConnection)
        let insertedRowID: Int64? = (lastInsertedRowID == 0) ? nil : lastInsertedRowID
        return DatabaseChanges(changedRowCount: changedRowsAfter - changedRowsBefore, insertedRowID: insertedRowID)
    }
}


// =========================================================================
// MARK: - Functions

extension Database {
    
    /// Add or redefine an SQL function.
    ///
    ///     let fn = DatabaseFunction(name: "succ", argumentCount: 1) { databaseValues in
    ///         let dbv = databaseValues.first!
    ///         guard let int = dbv.value() as Int? else {
    ///             return nil
    ///         }
    ///         return int + 1
    ///     }
    ///     db.add(function: fn)
    ///     Int.fetchOne(db, "SELECT succ(1)")! // 2
    public func add(function: DatabaseFunction) {
        functions.remove(function)
        functions.insert(function)
        let functionPointer = unsafeBitCast(function, to: UnsafeMutablePointer<Void>.self)
        let code = sqlite3_create_function_v2(
            sqliteConnection,
            function.name,
            function.argumentCount,
            SQLITE_UTF8 | function.eTextRep,
            functionPointer,
            { (context, argc, argv) in
                let function = unsafeBitCast(sqlite3_user_data(context), to: DatabaseFunction.self)
                do {
                    let result = try function.function(argc, argv)
                    switch result.storage {
                    case .null:
                        sqlite3_result_null(context)
                    case .int64(let int64):
                        sqlite3_result_int64(context, int64)
                    case .double(let double):
                        sqlite3_result_double(context, double)
                    case .string(let string):
                        sqlite3_result_text(context, string, -1, SQLITE_TRANSIENT)
                    case .blob(let data):
                        sqlite3_result_blob(context, data.bytes, Int32(data.length), SQLITE_TRANSIENT)
                    }
                } catch let error as DatabaseError {
                    if let message = error.message {
                        sqlite3_result_error(context, message, -1)
                    }
                    sqlite3_result_error_code(context, Int32(error.code))
                } catch {
                    sqlite3_result_error(context, "\(error)", -1)
                }
            }, nil, nil, nil)
        
        guard code == SQLITE_OK else {
            fatalError(DatabaseError(code: code, message: lastErrorMessage).description)
        }
    }
    
    /// Remove an SQL function.
    public func remove(function: DatabaseFunction) {
        functions.remove(function)
        let code = sqlite3_create_function_v2(
            sqliteConnection,
            function.name,
            function.argumentCount,
            SQLITE_UTF8 | function.eTextRep,
            nil, nil, nil, nil, nil)
        guard code == SQLITE_OK else {
            fatalError(DatabaseError(code: code, message: lastErrorMessage).description)
        }
    }
}


/// An SQL function.
public final class DatabaseFunction {
    public let name: String
    let argumentCount: Int32
    let pure: Bool
    let function: (Int32, UnsafeMutablePointer<OpaquePointer?>) throws -> DatabaseValue
    var eTextRep: Int32 { return pure ? SQLITE_DETERMINISTIC : 0 }
    
    /// Returns an SQL function.
    ///
    ///     let fn = DatabaseFunction(name: "succ", argumentCount: 1) { databaseValues in
    ///         let dbv = databaseValues.first!
    ///         guard let int = dbv.value() as Int? else {
    ///             return nil
    ///         }
    ///         return int + 1
    ///     }
    ///     db.add(function: fn)
    ///     Int.fetchOne(db, "SELECT succ(1)")! // 2
    ///
    /// - parameters:
    ///     - name: The function name.
    ///     - argumentCount: The number of arguments of the function. If
    ///       omitted, or nil, the function accepts any number of arguments.
    ///     - pure: Whether the function is "pure", which means that its results
    ///       only depends on its inputs. When a function is pure, SQLite has
    ///       the opportunity to perform additional optimizations. Default value
    ///       is false.
    ///     - function: A function that takes an array of DatabaseValue
    ///       arguments, and returns an optional DatabaseValueConvertible such
    ///       as Int, String, NSDate, etc. The array is guaranteed to have
    ///       exactly *argumentCount* elements, provided *argumentCount* is
    ///       not nil.
    public init(name: String, argumentCount: Int32? = nil, pure: Bool = false, function: [DatabaseValue] throws -> DatabaseValueConvertible?) {
        self.name = name
        self.argumentCount = argumentCount ?? -1
        self.pure = pure
        self.function = { (argc, argv) in
            let arguments = (0..<Int(argc)).map { index in DatabaseValue(sqliteValue: argv[index]!) }
            return try function(arguments)?.databaseValue ?? .null
        }
    }
}

extension DatabaseFunction : Hashable {
    /// The hash value.
    public var hashValue: Int {
        return name.hashValue ^ argumentCount.hashValue
    }
}

/// Two functions are equal if they share the same name and argumentCount.
public func ==(lhs: DatabaseFunction, rhs: DatabaseFunction) -> Bool {
    return lhs.name == rhs.name && lhs.argumentCount == rhs.argumentCount
}


// =========================================================================
// MARK: - Collations

extension Database {
    
    /// Add or redefine a collation.
    ///
    ///     let collation = DatabaseCollation(name: "localized_standard") { (string1, string2) in
    ///         return (string1 as NSString).localizedStandardCompare(string2)
    ///     }
    ///     db.add(collation: collation)
    ///     try db.execute("CREATE TABLE files (name TEXT COLLATE localized_standard")
    public func add(collation: DatabaseCollation) {
        collations.remove(collation)
        collations.insert(collation)
        let collationPointer = unsafeBitCast(collation, to: UnsafeMutablePointer<Void>.self)
        let code = sqlite3_create_collation_v2(
            sqliteConnection,
            collation.name,
            SQLITE_UTF8,
            collationPointer,
            { (collationPointer, length1, buffer1, length2, buffer2) -> Int32 in
                let collation = unsafeBitCast(collationPointer, to: DatabaseCollation.self)
                return Int32(collation.function(length1, buffer1, length2, buffer2).rawValue)
            }, nil)
        guard code == SQLITE_OK else {
            fatalError(DatabaseError(code: code, message: lastErrorMessage).description)
        }
    }
    
    /// Remove a collation.
    public func remove(collation: DatabaseCollation) {
        collations.remove(collation)
        sqlite3_create_collation_v2(
            sqliteConnection,
            collation.name,
            SQLITE_UTF8,
            nil, nil, nil)
    }
}

/// A Collation is a string comparison function used by SQLite.
public final class DatabaseCollation {
    public let name: String
    let function: (Int32, UnsafePointer<Void>, Int32, UnsafePointer<Void>) -> NSComparisonResult
    
    /// Returns a collation.
    ///
    ///     let collation = DatabaseCollation(name: "localized_standard") { (string1, string2) in
    ///         return (string1 as NSString).localizedStandardCompare(string2)
    ///     }
    ///     db.add(collation: collation)
    ///     try db.execute("CREATE TABLE files (name TEXT COLLATE localized_standard")
    ///
    /// - parameters:
    ///     - name: The function name.
    ///     - function: A function that compares two strings.
    public init(name: String, function: (String, String) -> NSComparisonResult) {
        self.name = name
        self.function = { (length1, buffer1, length2, buffer2) in
            // Buffers are not C strings: they do not end with \0.
            let string1 = String(bytesNoCopy: UnsafeMutablePointer<Void>(buffer1), length: Int(length1), encoding: NSUTF8StringEncoding, freeWhenDone: false)!
            let string2 = String(bytesNoCopy: UnsafeMutablePointer<Void>(buffer2), length: Int(length2), encoding: NSUTF8StringEncoding, freeWhenDone: false)!
            return function(string1, string2)
        }
    }
}

extension DatabaseCollation : Hashable {
    /// The hash value.
    public var hashValue: Int {
        // We can't compute a hash since the equality is based on the opaque
        // sqlite3_strnicmp SQLite function.
        return 0
    }
}

/// Two collations are equal if they share the same name (case insensitive)
public func ==(lhs: DatabaseCollation, rhs: DatabaseCollation) -> Bool {
    // See https://www.sqlite.org/c3ref/create_collation.html
    return sqlite3_stricmp(lhs.name, lhs.name) == 0
}


// =========================================================================
// MARK: - Encryption

#if SQLITE_HAS_CODEC
extension Database {
    private class func encrypt(sqliteConnection: SQLiteConnection, withPassphrase passphrase: String) throws {
        let data = passphrase.data(using: NSUTF8StringEncoding)!
        let code = sqlite3_key(sqliteConnection, data.bytes, Int32(data.length))
        guard code == SQLITE_OK else {
            throw DatabaseError(code: code, message: String(validatingUTF8: sqlite3_errmsg(sqliteConnection)))
        }
    }

    func encrypt(newPassphrase passphrase: String) throws {
        let data = passphrase.data(using: NSUTF8StringEncoding)!
        let code = sqlite3_rekey(sqliteConnection, data.bytes, Int32(data.length))
        guard code == SQLITE_OK else {
            throw DatabaseError(code: code, message: String(validatingUTF8: sqlite3_errmsg(sqliteConnection)))
        }
    }
}
#endif


// =========================================================================
// MARK: - Database Schema

extension Database {
    
    /// Clears the database schema cache.
    ///
    /// You may need to clear the cache manually if the database schema is
    /// modified by another connection.
    public func clearSchemaCache() {
        preconditionValidQueue()
        schemaCache.clear()
    }
    
    /// Returns whether a table exists.
    public func tableExists(_ tableName: String) -> Bool {
        preconditionValidQueue()
        
        // SQlite identifiers are case-insensitive, case-preserving (http://www.alberton.info/dbms_identifiers_and_case_sensitivity.html)
        return Row.fetchOne(self,
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND LOWER(name) = ?",
            arguments: [tableName.lowercased()]) != nil
    }
    
    /// Return the primary key for table named `tableName`.
    /// Throws if table does not exist.
    ///
    /// This method is not thread-safe.
    ///
    /// - throws: A DatabaseError if table does not exist.
    func primaryKey(forTableName tableName: String) throws -> PrimaryKey {
        if let primaryKey = schemaCache.primaryKey(tableName) {
            return primaryKey
        }
        
        // https://www.sqlite.org/pragma.html
        //
        // > PRAGMA database.table_info(table-name);
        // >
        // > This pragma returns one row for each column in the named table.
        // > Columns in the result set include the column name, data type,
        // > whether or not the column can be NULL, and the default value for
        // > the column. The "pk" column in the result set is zero for columns
        // > that are not part of the primary key, and is the index of the
        // > column in the primary key for columns that are part of the primary
        // > key.
        //
        // CREATE TABLE persons (
        //   id INTEGER PRIMARY KEY,
        //   firstName TEXT,
        //   lastName TEXT)
        //
        // PRAGMA table_info("persons")
        //
        // cid | name      | type    | notnull | dflt_value | pk |
        // 0   | id        | INTEGER | 0       | NULL       | 1  |
        // 1   | firstName | TEXT    | 0       | NULL       | 0  |
        // 2   | lastName  | TEXT    | 0       | NULL       | 0  |
        
        let columnInfos = ColumnInfo.fetchAll(self, "PRAGMA table_info(\(tableName.quotedDatabaseIdentifier))")
        guard columnInfos.count > 0 else {
            throw DatabaseError(message: "no such table: \(tableName)")
        }
        
        let primaryKey: PrimaryKey
        let pkColumnInfos = columnInfos
            .filter { $0.primaryKeyIndex > 0 }
            .sorted { $0.primaryKeyIndex < $1.primaryKeyIndex }
        
        switch pkColumnInfos.count {
        case 0:
            // No primary key column
            primaryKey = PrimaryKey.none
        case 1:
            // Single column
            let pkColumnInfo = pkColumnInfos.first!
            
            // https://www.sqlite.org/lang_createtable.html:
            //
            // > With one exception noted below, if a rowid table has a primary
            // > key that consists of a single column and the declared type of
            // > that column is "INTEGER" in any mixture of upper and lower
            // > case, then the column becomes an alias for the rowid. Such a
            // > column is usually referred to as an "integer primary key".
            // > A PRIMARY KEY column only becomes an integer primary key if the
            // > declared type name is exactly "INTEGER". Other integer type
            // > names like "INT" or "BIGINT" or "SHORT INTEGER" or "UNSIGNED
            // > INTEGER" causes the primary key column to behave as an ordinary
            // > table column with integer affinity and a unique index, not as
            // > an alias for the rowid.
            // >
            // > The exception mentioned above is that if the declaration of a
            // > column with declared type "INTEGER" includes an "PRIMARY KEY
            // > DESC" clause, it does not become an alias for the rowid [...]
            //
            // FIXME: We ignore the exception, and consider all INTEGER primary
            // keys as aliases for the rowid:
            if pkColumnInfo.type.uppercased() == "INTEGER" {
                primaryKey = .rowID(pkColumnInfo.name)
            } else {
                primaryKey = .regular([pkColumnInfo.name])
            }
        default:
            // Multi-columns primary key
            primaryKey = .regular(pkColumnInfos.map { $0.name })
        }
        
        schemaCache.setPrimaryKey(primaryKey, forTableName: tableName)
        return primaryKey
    }
    
    // CREATE TABLE persons (
    //   id INTEGER PRIMARY KEY,
    //   firstName TEXT,
    //   lastName TEXT)
    //
    // PRAGMA table_info("persons")
    //
    // cid | name      | type    | notnull | dflt_value | pk |
    // 0   | id        | INTEGER | 0       | NULL       | 1  |
    // 1   | firstName | TEXT    | 0       | NULL       | 0  |
    // 2   | lastName  | TEXT    | 0       | NULL       | 0  |
    private struct ColumnInfo : RowConvertible {
        let name: String
        let type: String
        let notNull: Bool
        let defaultDatabaseValue: DatabaseValue
        let primaryKeyIndex: Int
        
        init(_ row: Row) {
            name = row.value(named: "name")
            type = row.value(named: "type")
            notNull = row.value(named: "notnull")
            defaultDatabaseValue = row["dflt_value"]!
            primaryKeyIndex = row.value(named: "pk")
        }
    }
}

/// A primary key
enum PrimaryKey {
    
    /// No primary key
    case none
    
    /// An INTEGER PRIMARY KEY column that aliases the Row ID.
    /// Associated string is the column name.
    case rowID(String)
    
    /// Any primary key, but INTEGER PRIMARY KEY.
    /// Associated strings are column names.
    case regular([String])
    
    /// The columns in the primary key. May be empty.
    var columns: [String] {
        switch self {
        case .none:
            return []
        case .rowID(let column):
            return [column]
        case .regular(let columns):
            return columns
        }
    }
    
    /// The name of the INTEGER PRIMARY KEY
    var rowIDColumn: String? {
        switch self {
        case .none:
            return nil
        case .rowID(let column):
            return column
        case .regular:
            return nil
        }
    }
}


// =========================================================================
// MARK: - StatementCompilationObserver

// A class that uses sqlite3_set_authorizer to fetch information about a statement.
final class StatementCompilationObserver {
    let database: Database
    var sourceTables: Set<String> = []
    var invalidatesDatabaseSchemaCache = false
    
    init(_ database: Database) {
        self.database = database
    }
    
    func start() {
        let observerPointer = unsafeBitCast(self, to: UnsafeMutablePointer<Void>.self)
        sqlite3_set_authorizer(database.sqliteConnection, { (observerPointer, actionCode, CString1, CString2, CString3, CString4) -> Int32 in
            switch actionCode {
            case SQLITE_DROP_TABLE, SQLITE_DROP_TEMP_TABLE, SQLITE_DROP_TEMP_VIEW, SQLITE_DROP_VIEW, SQLITE_DETACH, SQLITE_ALTER_TABLE, SQLITE_DROP_VTABLE:
                let observer = unsafeBitCast(observerPointer, to: StatementCompilationObserver.self)
                observer.invalidatesDatabaseSchemaCache = true
            case SQLITE_READ:
                let observer = unsafeBitCast(observerPointer, to: StatementCompilationObserver.self)
                observer.sourceTables.insert(String(validatingUTF8: CString1)!)
            default:
                break
            }
            return SQLITE_OK
            }, observerPointer)
    }
    
    func stop() {
        sqlite3_set_authorizer(database.sqliteConnection, nil, nil)
    }
    
    func reset() {
        sourceTables = []
        invalidatesDatabaseSchemaCache = false
    }
}


// =========================================================================
// MARK: - Transactions

extension Database {
    /// Executes a block inside a database transaction.
    ///
    ///     try dbQueue.inDatabase do {
    ///         try db.inTransaction {
    ///             try db.execute("INSERT ...")
    ///             return .commit
    ///         }
    ///     }
    ///
    /// If the block throws an error, the transaction is rollbacked and the
    /// error is rethrown.
    ///
    /// This method is not reentrant: you can't nest transactions.
    ///
    /// - parameters:
    ///     - kind: The transaction type (default nil). If nil, the transaction
    ///       type is configuration.defaultTransactionKind, which itself
    ///       defaults to .immediate. See https://www.sqlite.org/lang_transaction.html
    ///       for more information.
    ///     - block: A block that executes SQL statements and return either
    ///       .commit or .rollback.
    /// - throws: The error thrown by the block.
    public func inTransaction(_ kind: TransactionKind? = nil, @noescape _ block: () throws -> TransactionCompletion) throws {
        try beginTransaction(kind ?? configuration.defaultTransactionKind)
        
        var completion: TransactionCompletion = .rollback
        var blockError: ErrorProtocol? = nil
        do {
            completion = try block()
        } catch {
            completion = .rollback
            blockError = error
        }
        
        switch completion {
        case .commit:
            try commit()
        case .rollback:
            // https://www.sqlite.org/lang_transaction.html#immediate
            //
            // > Response To Errors Within A Transaction
            // >
            // > If certain kinds of errors occur within a transaction, the
            // > transaction may or may not be rolled back automatically. The
            // > errors that can cause an automatic rollback include:
            // >
            // > - SQLITE_FULL: database or disk full
            // > - SQLITE_IOERR: disk I/O error
            // > - SQLITE_BUSY: database in use by another process
            // > - SQLITE_NOMEM: out or memory
            // >
            // > [...] It is recommended that applications respond to the errors
            // > listed above by explicitly issuing a ROLLBACK command. If the
            // > transaction has already been rolled back automatically by the
            // > error response, then the ROLLBACK command will fail with an
            // > error, but no harm is caused by this.
            if let databaseError = blockError as? DatabaseError {
                switch Int32(databaseError.code) {
                case SQLITE_FULL, SQLITE_IOERR, SQLITE_BUSY, SQLITE_NOMEM:
                    do { try rollback() } catch { }
                default:
                    try rollback()
                }
            } else {
                try rollback()
            }
        }
        
        if let blockError = blockError {
            throw blockError
        }
    }
    
    private func beginTransaction(_ kind: TransactionKind) throws {
        switch kind {
        case .deferred:
            try execute("BEGIN DEFERRED TRANSACTION")
        case .immediate:
            try execute("BEGIN IMMEDIATE TRANSACTION")
        case .exclusive:
            try execute("BEGIN EXCLUSIVE TRANSACTION")
        }
    }
    
    private func rollback() throws {
        try execute("ROLLBACK TRANSACTION")
    }
    
    private func commit() throws {
        try execute("COMMIT TRANSACTION")
    }
    
    /// Add a transaction observer, so that it gets notified of all
    /// database changes.
    ///
    /// The transaction observer is weakly referenced: it is not retained, and
    /// stops getting notifications after it is deallocated.
    public func add(transactionObserver: TransactionObserver) {
        preconditionValidQueue()
        transactionObservers.append(WeakTransactionObserver(transactionObserver))
        if transactionObservers.count == 1 {
            installTransactionObserverHooks()
        }
    }
    
    /// Remove a transaction observer.
    public func remove(transactionObserver: TransactionObserver) {
        preconditionValidQueue()
        transactionObservers.removeFirst { $0.observer === transactionObserver }
        if transactionObservers.isEmpty {
            uninstallTransactionObserverHooks()
        }
    }
    
    private func cleanupTransactionObservers() {
        transactionObservers = transactionObservers.filter { $0.observer != nil }
        if transactionObservers.isEmpty {
            uninstallTransactionObserverHooks()
        }
    }
    
    func updateStatementDidFail() throws {
        // Reset transactionState before didRollback eventually executes
        // other statements.
        let transactionState = self.transactionState
        self.transactionState = .waitForTransactionCompletion
        
        switch transactionState {
        case .rollbackFromTransactionObserver(let error):
            didRollback()
            throw error
        default:
            break
        }
    }
    
    func updateStatementDidExecute() {
        // Reset transactionState before didCommit or didRollback eventually
        // execute other statements.
        let transactionState = self.transactionState
        self.transactionState = .waitForTransactionCompletion
        
        switch transactionState {
        case .commit:
            didCommit()
        case .rollback:
            didRollback()
        default:
            break
        }
    }
    
    private func willCommit() throws {
        for observer in transactionObservers.flatMap({ $0.observer }) {
            try observer.databaseWillCommit()
        }
    }
    
    private func didChange(withEvent event: DatabaseEvent) {
        for observer in transactionObservers.flatMap({ $0.observer }) {
            observer.databaseDidChange(withEvent: event)
        }
    }
    
    private func didCommit() {
        for observer in transactionObservers.flatMap({ $0.observer }) {
            observer.databaseDidCommit(self)
        }
        cleanupTransactionObservers()
    }
    
    private func didRollback() {
        for observer in transactionObservers.flatMap({ $0.observer }) {
            observer.databaseDidRollback(self)
        }
        cleanupTransactionObservers()
    }
    
    private func installTransactionObserverHooks() {
        let dbPointer = unsafeBitCast(self, to: UnsafeMutablePointer<Void>.self)
        
        sqlite3_update_hook(sqliteConnection, { (dbPointer, updateKind, databaseNameCString, tableNameCString, rowID) in
            let db = unsafeBitCast(dbPointer, to: Database.self)
            db.didChange(withEvent: DatabaseEvent(
                databaseNameCString: databaseNameCString,
                tableNameCString: tableNameCString,
                kind: DatabaseEvent.Kind(rawValue: updateKind)!,
                rowID: rowID))
            }, dbPointer)
        
        
        sqlite3_commit_hook(sqliteConnection, { dbPointer in
            let db = unsafeBitCast(dbPointer, to: Database.self)
            do {
                try db.willCommit()
                db.transactionState = .commit
                // Next step: updateStatementDidExecute()
                return 0
            } catch {
                db.transactionState = .rollbackFromTransactionObserver(error)
                // Next step: sqlite3_rollback_hook callback
                return 1
            }
            }, dbPointer)
        
        
        sqlite3_rollback_hook(sqliteConnection, { dbPointer in
            let db = unsafeBitCast(dbPointer, to: Database.self)
            switch db.transactionState {
            case .rollbackFromTransactionObserver:
                // Next step: updateStatementDidFail()
                break
            default:
                db.transactionState = .rollback
                // Next step: updateStatementDidExecute()
            }
            }, dbPointer)
    }
    
    private func uninstallTransactionObserverHooks() {
        sqlite3_update_hook(sqliteConnection, nil, nil)
        sqlite3_commit_hook(sqliteConnection, nil, nil)
        sqlite3_rollback_hook(sqliteConnection, nil, nil)
    }
}


/// An SQLite transaction kind. See https://www.sqlite.org/lang_transaction.html
public enum TransactionKind {
    case deferred
    case immediate
    case exclusive
}


/// The end of a transaction: Commit, or Rollback
public enum TransactionCompletion {
    case commit
    case rollback
}

/// The states that keep track of transaction completions in order to notify
/// transaction observers.
private enum TransactionState {
    case waitForTransactionCompletion
    case commit
    case rollback
    case rollbackFromTransactionObserver(ErrorProtocol)
}

/// A transaction observer is notified of all changes and transactions committed
/// or rollbacked on a database.
///
/// Adopting types must be a class.
public protocol TransactionObserver : class {
    
    /// Notifies a database change (insert, update, or delete).
    ///
    /// The change is pending until the end of the current transaction, notified
    /// to databaseWillCommit, databaseDidCommit and databaseDidRollback.
    ///
    /// This method is called on the database queue.
    ///
    /// The event is only valid for the duration of this method call. If you
    /// need to keep it longer, store a copy of its properties.
    ///
    /// - warning: this method must not change the database.
    func databaseDidChange(withEvent event: DatabaseEvent)
    
    /// When a transaction is about to be committed, the transaction observer
    /// has an opportunity to rollback pending changes by throwing an error.
    ///
    /// This method is called on the database queue.
    ///
    /// - warning: this method must not change the database.
    ///
    /// - throws: An eventual error that rollbacks pending changes.
    func databaseWillCommit() throws
    
    /// Database changes have been committed.
    ///
    /// This method is called on the database queue. It can change the database.
    func databaseDidCommit(_ db: Database)
    
    /// Database changes have been rollbacked.
    ///
    /// This method is called on the database queue. It can change the database.
    func databaseDidRollback(_ db: Database)
}

class WeakTransactionObserver {
    weak var observer: TransactionObserver?
    init(_ observer: TransactionObserver) {
        self.observer = observer
    }
}


/// A database event, notified to TransactionObserver.
///
/// See https://www.sqlite.org/c3ref/update_hook.html for more information.
public struct DatabaseEvent {
    private let databaseNameCString: UnsafePointer<Int8>
    private let tableNameCString: UnsafePointer<Int8>
    
    /// An event kind
    public enum Kind: Int32 {
        case insert = 18    // SQLITE_INSERT
        case delete = 9     // SQLITE_DELETE
        case update = 23    // SQLITE_UPDATE
    }
    
    /// The event kind
    public let kind: Kind
    
    /// The database name
    public var databaseName: String { return String(validatingUTF8: databaseNameCString)! }

    /// The table name
    public var tableName: String { return String(validatingUTF8: tableNameCString)! }
    
    /// The rowID of the changed row.
    public let rowID: Int64
}
