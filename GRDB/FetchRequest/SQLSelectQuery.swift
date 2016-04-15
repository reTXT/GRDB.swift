

// MARK: - _SQLSelectQuery

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public struct _SQLSelectQuery {
    var selection: [_SQLSelectable]
    var distinct: Bool
    var source: _SQLSource?
    var whereExpression: _SQLExpression?
    var groupByExpressions: [_SQLExpression]
    var orderings: [_SQLOrdering]
    var reversed: Bool
    var havingExpression: _SQLExpression?
    var limit: _SQLLimit?
    
    init(
        select selection: [_SQLSelectable],
        distinct: Bool = false,
        from source: _SQLSource? = nil,
        filter whereExpression: _SQLExpression? = nil,
        groupBy groupByExpressions: [_SQLExpression] = [],
        orderBy orderings: [_SQLOrdering] = [],
        reversed: Bool = false,
        having havingExpression: _SQLExpression? = nil,
        limit: _SQLLimit? = nil)
    {
        self.selection = selection
        self.distinct = distinct
        self.source = source
        self.whereExpression = whereExpression
        self.groupByExpressions = groupByExpressions
        self.orderings = orderings
        self.reversed = reversed
        self.havingExpression = havingExpression
        self.limit = limit
    }
    
    func sql(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        var sql = "SELECT"
        
        if distinct {
            sql += " DISTINCT"
        }
        
        assert(!selection.isEmpty)
        sql += try " " + selection.map { try $0.resultColumnSQL(db, &bindings) }.joined(separator: ", ")
        
        if let source = source {
            sql += try " FROM " + source.sql(db, &bindings)
        }
        
        if let whereExpression = whereExpression {
            sql += try " WHERE " + whereExpression.sql(db, &bindings)
        }
        
        if !groupByExpressions.isEmpty {
            sql += try " GROUP BY " + groupByExpressions.map { try $0.sql(db, &bindings) }.joined(separator: ", ")
        }
        
        if let havingExpression = havingExpression {
            sql += try " HAVING " + havingExpression.sql(db, &bindings)
        }
        
        var orderings = self.orderings
        if reversed {
            if orderings.isEmpty {
                guard let source = source, case .table(let tableName, let alias) = source else {
                    throw DatabaseError(message: "can't reverse without a source table")
                }
                guard case let columns = try db.primaryKey(forTableName: tableName).columns where !columns.isEmpty else {
                    throw DatabaseError(message: "can't reverse a table without primary key")
                }
                orderings = columns.map { _SQLExpression.identifier(identifier: $0, sourceName: alias).desc }
            } else {
                orderings = orderings.map { $0.reversedOrdering }
            }
        }
        if !orderings.isEmpty {
            sql += try " ORDER BY " + orderings.map { try $0.orderingSQL(db, &bindings) }.joined(separator: ", ")
        }
        
        if let limit = limit {
            sql += " LIMIT " + limit.sql
        }
        
        return sql
    }
    
    /// Returns a query that counts the number of rows matched by self.
    var countQuery: _SQLSelectQuery {
        guard groupByExpressions.isEmpty && limit == nil else {
            // SELECT ... GROUP BY ...
            // SELECT ... LIMIT ...
            return trivialCountQuery
        }
        
        guard let source = source, case .table(name: let tableName, alias: let alias) = source else {
            // SELECT ... FROM (something which is not a table)
            return trivialCountQuery
        }
        
        assert(!selection.isEmpty)
        if selection.count == 1 {
            let selectable = self.selection[0]
            switch selectable.sqlSelectableKind {
            case .star(sourceName: let sourceName):
                guard !distinct else {
                    return trivialCountQuery
                }
                
                if let sourceName = sourceName {
                    guard sourceName == tableName || sourceName == alias else {
                        return trivialCountQuery
                    }
                }
                
                // SELECT * FROM tableName ...
                // ->
                // SELECT COUNT(*) FROM tableName ...
                var countQuery = unorderedQuery
                countQuery.selection = [_SQLExpression.count(selectable)]
                return countQuery
                
            case .expression(let expression):
                // SELECT [DISTINCT] expr FROM tableName ...
                if distinct {
                    // SELECT DISTINCT expr FROM tableName ...
                    // ->
                    // SELECT COUNT(DISTINCT expr) FROM tableName ...
                    var countQuery = unorderedQuery
                    countQuery.distinct = false
                    countQuery.selection = [_SQLExpression.countDistinct(expression)]
                    return countQuery
                } else {
                    // SELECT expr FROM tableName ...
                    // ->
                    // SELECT COUNT(*) FROM tableName ...
                    var countQuery = unorderedQuery
                    countQuery.selection = [_SQLExpression.count(_SQLResultColumn.star(nil))]
                    return countQuery
                }
            }
        } else {
            // SELECT [DISTINCT] expr1, expr2, ... FROM tableName ...
            
            guard !distinct else {
                return trivialCountQuery
            }

            // SELECT expr1, expr2, ... FROM tableName ...
            // ->
            // SELECT COUNT(*) FROM tableName ...
            var countQuery = unorderedQuery
            countQuery.selection = [_SQLExpression.count(_SQLResultColumn.star(nil))]
            return countQuery
        }
    }
    
    // SELECT COUNT(*) FROM (self)
    private var trivialCountQuery: _SQLSelectQuery {
        return _SQLSelectQuery(
            select: [_SQLExpression.count(_SQLResultColumn.star(nil))],
            from: .query(query: unorderedQuery, alias: nil))
    }
    
    /// Remove ordering
    private var unorderedQuery: _SQLSelectQuery {
        var query = self
        query.reversed = false
        query.orderings = []
        return query
    }
}


// MARK: - _SQLSource

indirect enum _SQLSource {
    case table(name: String, alias: String?)
    case query(query: _SQLSelectQuery, alias: String?)
    
    func sql(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        switch self {
        case .table(let table, let alias):
            if let alias = alias {
                return table.quotedDatabaseIdentifier + " AS " + alias.quotedDatabaseIdentifier
            } else {
                return table.quotedDatabaseIdentifier
            }
        case .query(let query, let alias):
            if let alias = alias {
                return try "(" + query.sql(db, &bindings) + ") AS " + alias.quotedDatabaseIdentifier
            } else {
                return try "(" + query.sql(db, &bindings) + ")"
            }
        }
    }
}


// MARK: - _SQLOrdering

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SQLOrdering {
    var reversedOrdering: _SQLOrderingExpression { get }
    func orderingSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String
}

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public enum _SQLOrderingExpression {
    case asc(_SQLExpression)
    case desc(_SQLExpression)
}

extension _SQLOrderingExpression : _SQLOrdering {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var reversedOrdering: _SQLOrderingExpression {
        switch self {
        case .asc(let expression):
            return .desc(expression)
        case .desc(let expression):
            return .asc(expression)
        }
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func orderingSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        switch self {
        case .asc(let expression):
            return try expression.sql(db, &bindings) + " ASC"
        case .desc(let expression):
            return try expression.sql(db, &bindings) + " DESC"
        }
    }
}


// MARK: - _SQLLimit

struct _SQLLimit {
    let limit: Int
    let offset: Int?
    
    var sql: String {
        if let offset = offset {
            return "\(limit) OFFSET \(offset)"
        } else {
            return "\(limit)"
        }
    }
}


// MARK: - _SQLExpressible

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    var sqlExpression: _SQLExpression { get }
}

// Conformance to _SQLExpressible
extension DatabaseValueConvertible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return .value(self)
    }
}

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _PrivateSQLExpressible : _SQLExpressible, _SQLOrdering, _SQLSelectable {
    // _SQLExpressible can be adopted by Swift standard types, and user types,
    // through the DatabaseValueConvertible protocol, which inherits
    // from _SQLExpressible.
    //
    // For example, Int adopts _SQLExpressible through DatabaseValueConvertible.
    //
    // _PrivateSQLExpressible, on the other side, is not adopted by any
    // Swift standard type or any user type. It is only adopted by GRDB types,
    // such as SQLColumn, _SQLExpression and _SQLLiteral.
    //
    // This separation lets us define functions and operators that do not
    // spill out. The three declarations below have no chance overloading a
    // Swift-defined operator, or a user-defined operator:
    //
    // - ==(_SQLExpressible, _PrivateSQLExpressible)
    // - ==(_PrivateSQLExpressible, _SQLExpressible)
    // - ==(_PrivateSQLExpressible, _PrivateSQLExpressible)
}

// Conformance to _SQLOrdering
extension _PrivateSQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var reversedOrdering: _SQLOrderingExpression {
        return .desc(sqlExpression)
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func orderingSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        return try sqlExpression.sql(db, &bindings)
    }
}

// Conformance to _SQLSelectable
extension _PrivateSQLExpressible {
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func resultColumnSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        return try sqlExpression.sql(db, &bindings)
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func countedSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        return try sqlExpression.sql(db, &bindings)
    }
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlSelectableKind: _SQLSelectableKind {
        return .expression(sqlExpression)
    }
}

extension _PrivateSQLExpressible {
    
    /// Returns a value that can be used as an argument to FetchRequest.order()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var asc: _SQLOrderingExpression {
        return .asc(sqlExpression)
    }
    
    /// Returns a value that can be used as an argument to FetchRequest.order()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var desc: _SQLOrderingExpression {
        return .desc(sqlExpression)
    }
    
    /// Returns a value that can be used as an argument to FetchRequest.select()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func aliased(alias: String) -> _SQLSelectable {
        return _SQLResultColumn.expression(expression: sqlExpression, alias: alias)
    }
}


/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public indirect enum _SQLExpression {
    /// For example: `name || 'rrr' AS pirateName`
    case SQLLiteral(String)
    
    /// For example: `1` or `'foo'`
    case value(DatabaseValueConvertible?)
    
    /// For example: `name`, `table.name`
    case identifier(identifier: String, sourceName: String?)
    
    /// For example: `name = 'foo' COLLATE NOCASE`
    case collate(_SQLExpression, String)
    
    /// For example: `NOT condition`
    case not(_SQLExpression)
    
    /// For example: `name = 'foo'`
    case equal(_SQLExpression, _SQLExpression)
    
    /// For example: `name <> 'foo'`
    case notEqual(_SQLExpression, _SQLExpression)
    
    /// For example: `name IS NULL`
    case isOperator(_SQLExpression, _SQLExpression)
    
    /// For example: `name IS NOT NULL`
    case isNot(_SQLExpression, _SQLExpression)
    
    /// For example: `-value`
    case prefixOperator(String, _SQLExpression)
    
    /// For example: `age + 1`
    case infixOperator(String, _SQLExpression, _SQLExpression)
    
    /// For example: `id IN (1,2,3)`
    case inOperator([_SQLExpression], _SQLExpression)
    
    /// For example `id IN (SELECT ...)`
    case inSubQuery(_SQLSelectQuery, _SQLExpression)
    
    /// For example `EXISTS (SELECT ...)`
    case exists(_SQLSelectQuery)
    
    /// For example: `age BETWEEN 1 AND 2`
    case between(value: _SQLExpression, min: _SQLExpression, max: _SQLExpression)
    
    /// For example: `LOWER(name)`
    case function(String, [_SQLExpression])
    
    /// For example: `COUNT(*)`
    case count(_SQLSelectable)
    
    /// For example: `COUNT(DISTINCT name)`
    case countDistinct(_SQLExpression)
    
    ///
    func sql(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        switch self {
        case .SQLLiteral(let sql):
            return sql
            
        case .value(let value):
            guard let value = value else {
                return "NULL"
            }
            bindings.append(value)
            return "?"
            
        case .identifier(let identifier, let sourceName):
            if let sourceName = sourceName {
                return sourceName.quotedDatabaseIdentifier + "." + identifier.quotedDatabaseIdentifier
            } else {
                return identifier.quotedDatabaseIdentifier
            }
            
        case .collate(let expression, let collation):
            let sql = try expression.sql(db, &bindings)
            let chars = sql.characters
            if chars.last! == ")" {
                return String(chars.prefix(upTo: chars.endIndex.predecessor())) + " COLLATE " + collation + ")"
            } else {
                return sql + " COLLATE " + collation
            }
            
        case .not(let condition):
            switch condition {
            case .not(let expression):
                return try expression.sql(db, &bindings)
                
            case .inOperator(let expressions, let expression):
                if expressions.isEmpty {
                    return "1"
                } else {
                    return try "(" + expression.sql(db, &bindings) + " NOT IN (" + expressions.map { try $0.sql(db, &bindings) }.joined(separator: ", ") + "))"
                }
                
            case .inSubQuery(let subQuery, let expression):
                return try "(" + expression.sql(db, &bindings) + " NOT IN (" + subQuery.sql(db, &bindings)  + "))"
                
            case .exists(let subQuery):
                return try "(NOT EXISTS (" + subQuery.sql(db, &bindings)  + "))"
                
            case .equal(let lhs, let rhs):
                return try _SQLExpression.notEqual(lhs, rhs).sql(db, &bindings)
                
            case .notEqual(let lhs, let rhs):
                return try _SQLExpression.equal(lhs, rhs).sql(db, &bindings)
                
            case .isOperator(let lhs, let rhs):
                return try _SQLExpression.isNot(lhs, rhs).sql(db, &bindings)
                
            case .isNot(let lhs, let rhs):
                return try _SQLExpression.isOperator(lhs, rhs).sql(db, &bindings)
                
            default:
                return try "(NOT " + condition.sql(db, &bindings) + ")"
            }
            
        case .equal(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .value(let rhs)) where rhs == nil:
                // Swiftism!
                // Turn `filter(a == nil)` into `a IS NULL` since the intention is obviously to check for NULL. `a = NULL` would evaluate to NULL.
                return try "(" + lhs.sql(db, &bindings) + " IS NULL)"
            case (.value(let lhs), let rhs) where lhs == nil:
                // Swiftism!
                return try "(" + rhs.sql(db, &bindings) + " IS NULL)"
            default:
                return try "(" + lhs.sql(db, &bindings) + " = " + rhs.sql(db, &bindings) + ")"
            }
            
        case .notEqual(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .value(let rhs)) where rhs == nil:
                // Swiftism!
                // Turn `filter(a != nil)` into `a IS NOT NULL` since the intention is obviously to check for NULL. `a <> NULL` would evaluate to NULL.
                return try "(" + lhs.sql(db, &bindings) + " IS NOT NULL)"
            case (.value(let lhs), let rhs) where lhs == nil:
                // Swiftism!
                return try "(" + rhs.sql(db, &bindings) + " IS NOT NULL)"
            default:
                return try "(" + lhs.sql(db, &bindings) + " <> " + rhs.sql(db, &bindings) + ")"
            }
            
        case .isOperator(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .value(let rhs)) where rhs == nil:
                return try "(" + lhs.sql(db, &bindings) + " IS NULL)"
            case (.value(let lhs), let rhs) where lhs == nil:
                return try "(" + rhs.sql(db, &bindings) + " IS NULL)"
            default:
                return try "(" + lhs.sql(db, &bindings) + " IS " + rhs.sql(db, &bindings) + ")"
            }
            
        case .isNot(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .value(let rhs)) where rhs == nil:
                return try "(" + lhs.sql(db, &bindings) + " IS NOT NULL)"
            case (.value(let lhs), let rhs) where lhs == nil:
                return try "(" + rhs.sql(db, &bindings) + " IS NOT NULL)"
            default:
                return try "(" + lhs.sql(db, &bindings) + " IS NOT " + rhs.sql(db, &bindings) + ")"
            }
            
        case .prefixOperator(let SQLOperator, let value):
            return try SQLOperator + value.sql(db, &bindings)
            
        case .infixOperator(let SQLOperator, let lhs, let rhs):
            return try "(" + lhs.sql(db, &bindings) + " \(SQLOperator) " + rhs.sql(db, &bindings) + ")"
            
        case .inOperator(let expressions, let expression):
            guard !expressions.isEmpty else {
                return "0"
            }
            return try "(" + expression.sql(db, &bindings) + " IN (" + expressions.map { try $0.sql(db, &bindings) }.joined(separator: ", ")  + "))"
        
        case .inSubQuery(let subQuery, let expression):
            return try "(" + expression.sql(db, &bindings) + " IN (" + subQuery.sql(db, &bindings)  + "))"
            
        case .exists(let subQuery):
            return try "(EXISTS (" + subQuery.sql(db, &bindings)  + "))"
            
        case .between(value: let value, min: let min, max: let max):
            return try "(" + value.sql(db, &bindings) + " BETWEEN " + min.sql(db, &bindings) + " AND " + max.sql(db, &bindings) + ")"
            
        case .function(let functionName, let functionArguments):
            return try functionName + "(" + functionArguments.map { try $0.sql(db, &bindings) }.joined(separator: ", ")  + ")"
            
        case .count(let counted):
            return try "COUNT(" + counted.countedSQL(db, &bindings) + ")"
            
        case .countDistinct(let expression):
            return try "COUNT(DISTINCT " + expression.sql(db, &bindings) + ")"
        }
    }
}

extension _SQLExpression : _PrivateSQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return self
    }
}


// MARK: - _SQLSelectable

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SQLSelectable {
    func resultColumnSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String
    func countedSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String
    var sqlSelectableKind: _SQLSelectableKind { get }
}

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public enum _SQLSelectableKind {
    case expression(_SQLExpression)
    case star(sourceName: String?)
}

enum _SQLResultColumn {
    case expression(expression: _SQLExpression, alias: String)
    case star(String?)
}

extension _SQLResultColumn : _SQLSelectable {
    
    func resultColumnSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        switch self {
        case .star(let sourceName):
            if let sourceName = sourceName {
                return sourceName.quotedDatabaseIdentifier + ".*"
            } else {
                return "*"
            }
        case .expression(expression: let expression, alias: let alias):
            return try expression.sql(db, &bindings) + " AS " + alias.quotedDatabaseIdentifier
        }
    }
    
    func countedSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        switch self {
        case .star:
            return "*"
        case .expression(expression: let expression, alias: _):
            return try expression.sql(db, &bindings)
        }
    }
    
    var sqlSelectableKind: _SQLSelectableKind {
        switch self {
        case .star(let sourceName):
            return .star(sourceName: sourceName)
        case .expression(expression: let expression, alias: _):
            return .expression(expression)
        }
    }
}


// MARK: _SQLLiteral

struct _SQLLiteral {
    let sql: String
    init(_ sql: String) {
        self.sql = sql
    }
}

extension _SQLLiteral : _PrivateSQLExpressible {
    var sqlExpression: _SQLExpression {
        return .SQLLiteral(sql)
    }
}


// MARK: - SQLColumn

/// A column in the database
///
/// See https://github.com/groue/GRDB.swift#the-query-interface
public struct SQLColumn {
    let sourceName: String?
    
    /// The name of the column
    public let name: String
    
    /// Initializes a column given its name.
    public init(_ name: String) {
        self.name = name
        self.sourceName = nil
    }
    
    init(_ name: String, sourceName: String?) {
        self.name = name
        self.sourceName = sourceName
    }
}

extension SQLColumn : _PrivateSQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return .identifier(identifier: name, sourceName: sourceName)
    }
}

