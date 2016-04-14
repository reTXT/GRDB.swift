/// The protocol for all database schema caches
protocol DatabaseSchemaCache {
    mutating func clear()
    
    func primaryKey(forTableName tableName: String) -> PrimaryKey?
    mutating func setPrimaryKey(_ primaryKey: PrimaryKey, forTableName tableName: String)

    func updateStatement(sql: String) -> UpdateStatement?
    mutating func setUpdateStatement(_ statement: UpdateStatement, forSQL sql: String)
    
    func selectStatement(sql: String) -> SelectStatement?
    mutating func setSelectStatement(_ statement: SelectStatement, forSQL sql: String)
}

/// A thread-unsafe database schema cache
class SimpleDatabaseSchemaCache: DatabaseSchemaCache {
    private var primaryKeys: [String: PrimaryKey] = [:]
    private var updateStatements: [String: UpdateStatement] = [:]
    private var selectStatements: [String: SelectStatement] = [:]
    
    func clear() {
        primaryKeys = [:]
        
        // We do clear updateStatementCache and selectStatementCache despite
        // the automatic statement recompilation (see https://www.sqlite.org/c3ref/prepare.html)
        // because the automatic statement recompilation only happens a
        // limited number of times.
        updateStatements = [:]
        selectStatements = [:]
    }
    
    func primaryKey(forTableName tableName: String) -> PrimaryKey? {
        return primaryKeys[tableName]
    }
    
    func setPrimaryKey(_ primaryKey: PrimaryKey, forTableName tableName: String) {
        primaryKeys[tableName] = primaryKey
    }
    
    func updateStatement(sql: String) -> UpdateStatement? {
        return updateStatements[sql]
    }
    
    func setUpdateStatement(_ statement: UpdateStatement, forSQL sql: String) {
        updateStatements[sql] = statement
    }
    
    func selectStatement(sql: String) -> SelectStatement? {
        return selectStatements[sql]
    }
    
    func setSelectStatement(_ statement: SelectStatement, forSQL sql: String) {
        selectStatements[sql] = statement
    }
}

/// A thread-safe database schema cache
struct SharedDatabaseSchemaCache: DatabaseSchemaCache {
    private let cache = ReadWriteBox(SimpleDatabaseSchemaCache())
    
    mutating func clear() {
        cache.write { $0.clear() }
    }
    
    func primaryKey(forTableName tableName: String) -> PrimaryKey? {
        return cache.read { $0.primaryKey(forTableName: tableName) }
    }
    
    mutating func setPrimaryKey(_ primaryKey: PrimaryKey, forTableName tableName: String) {
        cache.write { $0.setPrimaryKey(primaryKey, forTableName: tableName) }
    }
    
    func updateStatement(sql: String) -> UpdateStatement? {
        return cache.read { $0.updateStatement(sql: sql) }
    }
    
    mutating func setUpdateStatement(_ statement: UpdateStatement, forSQL sql: String) {
        cache.write { $0.setUpdateStatement(statement, forSQL: sql) }
    }
    
    func selectStatement(sql: String) -> SelectStatement? {
        return cache.read { $0.selectStatement(sql: sql) }
    }
    
    mutating func setSelectStatement(_ statement: SelectStatement, forSQL sql: String) {
        cache.write { $0.setSelectStatement(statement, forSQL: sql) }
    }
}
