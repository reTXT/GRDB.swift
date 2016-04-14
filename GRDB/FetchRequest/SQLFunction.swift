// MARK: - Custom Functions

extension DatabaseFunction {
    /// Returns an SQL expression that applies the function.
    ///
    /// See https://github.com/groue/GRDB.swift/#sql-functions
    public func apply(_ arguments: _SQLExpressible...) -> _SQLExpression {
        return .function(name, arguments.map { $0.sqlExpression })
    }
}


// MARK: - ABS(...)

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func abs(_ value: _PrivateSQLExpressible) -> _SQLExpression {
    return .function("ABS", [value.sqlExpression])
}


// MARK: - AVG(...)

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func average(_ value: _PrivateSQLExpressible) -> _SQLExpression {
    return .function("AVG", [value.sqlExpression])
}


// MARK: - COUNT(...)

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func count(_ counted: _PrivateSQLExpressible) -> _SQLExpression {
    return .count(counted)
}


// MARK: - COUNT(DISTINCT ...)

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func count(distinct value: _PrivateSQLExpressible) -> _SQLExpression {
    return .countDistinct(value.sqlExpression)
}


// MARK: - IFNULL(...)

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func ?? (lhs: _PrivateSQLExpressible, rhs: _SQLExpressible) -> _SQLExpression {
    return .function("IFNULL", [lhs.sqlExpression, rhs.sqlExpression])
}

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func ?? (lhs: _SQLExpressible?, rhs: _PrivateSQLExpressible) -> _SQLExpression {
    if let lhs = lhs {
        return .function("IFNULL", [lhs.sqlExpression, rhs.sqlExpression])
    } else {
        return rhs.sqlExpression
    }
}


// MARK: - MAX(...)

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func max(_ value: _PrivateSQLExpressible) -> _SQLExpression {
    return .function("MAX", [value.sqlExpression])
}


// MARK: - MIN(...)

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func min(_ value: _PrivateSQLExpressible) -> _SQLExpression {
    return .function("MIN", [value.sqlExpression])
}


// MARK: - SUM(...)

/// Returns an SQL expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-functions
public func sum(_ value: _PrivateSQLExpressible) -> _SQLExpression {
    return .function("SUM", [value.sqlExpression])
}


// MARK: - Swift String functions

extension _PrivateSQLExpressible {
    /// Returns an SQL expression that applies the Swift's built-in
    /// capitalized NSString property. It is NULL for non-String arguments.
    ///
    ///     let nameColumn = SQLColumn("name")
    ///     let request = Person.select(nameColumn.capitalized())
    ///     let names = String.fetchAll(dbQueue, request)   // [String]
    public func capitalized() -> _SQLExpression {
        // TODO: decide if it should be a property or a function.
        // TODO: Open a bugs.swift.org issue about String/NSString var/function inconsistencies
        return DatabaseFunction.capitalized.apply(sqlExpression)
    }

    /// Returns an SQL expression that applies the Swift's built-in
    /// lowercased String method. It is NULL for non-String arguments.
    ///
    ///     let nameColumn = SQLColumn("name")
    ///     let request = Person.select(nameColumn.lowercased())
    ///     let names = String.fetchAll(dbQueue, request)   // [String]
    public func lowercased() -> _SQLExpression {
        // TODO: decide if it should be a property or a function.
        return DatabaseFunction.lowercased.apply(sqlExpression)
    }

    /// Returns an SQL expression that applies the Swift's built-in
    /// uppercased() String property. It is NULL for non-String arguments.
    ///
    ///     let nameColumn = SQLColumn("name")
    ///     let request = Person.select(nameColumn.uppercased())
    ///     let names = String.fetchAll(dbQueue, request)   // [String]
    public func uppercased() -> _SQLExpression {
        // TODO: decide if it should be a property or a function.
        return DatabaseFunction.uppercased.apply(sqlExpression)
    }
}

@available(iOS 9.0, OSX 10.11, *)
extension _PrivateSQLExpressible {
    /// Returns an SQL expression that applies the Swift's built-in
    /// localizedCapitalized NSString property. It is NULL for non-String arguments.
    ///
    ///     let nameColumn = SQLColumn("name")
    ///     let request = Person.select(nameColumn.localizedCapitalized())
    ///     let names = String.fetchAll(dbQueue, request)   // [String]
    public func localizedCapitalized() -> _SQLExpression {
        // TODO: decide if it should be a property or a function.
        return DatabaseFunction.localizedCapitalized.apply(sqlExpression)
    }
    
    /// Returns an SQL expression that applies the Swift's built-in
    /// localizedLowercase NSString property. It is NULL for non-String arguments.
    ///
    ///     let nameColumn = SQLColumn("name")
    ///     let request = Person.select(nameColumn.localizedLowercase())
    ///     let names = String.fetchAll(dbQueue, request)   // [String]
    public func localizedLowercase() -> _SQLExpression {
        // TODO: decide if it should be a property or a function.
        return DatabaseFunction.localizedLowercase.apply(sqlExpression)
    }
    
    /// Returns an SQL expression that applies the Swift's built-in
    /// localizedUppercase String property. It is NULL for non-String arguments.
    ///
    ///     let nameColumn = SQLColumn("name")
    ///     let request = Person.select(nameColumn.localizedUppercase())
    ///     let names = String.fetchAll(dbQueue, request)   // [String]
    public func localizedUppercase() -> _SQLExpression {
        // TODO: decide if it should be a property or a function.
        return DatabaseFunction.localizedUppercase.apply(sqlExpression)
    }
}
