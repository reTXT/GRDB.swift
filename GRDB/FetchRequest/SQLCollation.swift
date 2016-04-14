/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public struct _SQLCollatedExpression {
    let baseExpression: _SQLExpression
    let collationName: String
}

extension _SQLCollatedExpression : _SQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return .collate(baseExpression, collationName)
    }
}

extension _SQLCollatedExpression : _SQLSortDescriptorType {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var reversedSortDescriptor: _SQLSortDescriptor {
        return .desc(sqlExpression)
    }
    
    /// Returns a value that can be used as an argument to FetchRequest.order()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var asc: _SQLSortDescriptor {
        return .asc(sqlExpression)
    }
    
    /// Returns a value that can be used as an argument to FetchRequest.order()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var desc: _SQLSortDescriptor {
        return .desc(sqlExpression)
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func orderingSQL(_ db: Database, _ bindings: inout [DatabaseValueConvertible?]) throws -> String {
        return try sqlExpression.orderingSQL(db, &bindings)
    }
}

extension _PrivateSQLExpressible {
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func collating(_ collationName: String) -> _SQLCollatedExpression {
        return _SQLCollatedExpression(baseExpression: sqlExpression, collationName: collationName)
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func collating(_ collation: DatabaseCollation) -> _SQLCollatedExpression {
        return collating(collation.name)
    }
}


// MARK: - Operator = COLLATE

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func == (lhs: _SQLCollatedExpression, rhs: _SQLExpressible?) -> _SQLExpression {
    return .collate(lhs.baseExpression == rhs, lhs.collationName)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func == (lhs: _SQLExpressible?, rhs: _SQLCollatedExpression) -> _SQLExpression {
    return .collate(lhs == rhs.baseExpression, rhs.collationName)
}


// MARK: - Operator != COLLATE

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func != (lhs: _SQLCollatedExpression, rhs: _SQLExpressible?) -> _SQLExpression {
    return .collate(lhs.baseExpression != rhs, lhs.collationName)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func != (lhs: _SQLExpressible?, rhs: _SQLCollatedExpression) -> _SQLExpression {
    return .collate(lhs != rhs.baseExpression, rhs.collationName)
}


// MARK: - Operator < COLLATE

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func < (lhs: _SQLCollatedExpression, rhs: _SQLExpressible) -> _SQLExpression {
    return .collate(lhs.baseExpression < rhs, lhs.collationName)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func < (lhs: _SQLExpressible, rhs: _SQLCollatedExpression) -> _SQLExpression {
    return .collate(lhs < rhs.baseExpression, rhs.collationName)
}


// MARK: - Operator <= COLLATE

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func <= (lhs: _SQLCollatedExpression, rhs: _SQLExpressible) -> _SQLExpression {
    return .collate(lhs.baseExpression <= rhs, lhs.collationName)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func <= (lhs: _SQLExpressible, rhs: _SQLCollatedExpression) -> _SQLExpression {
    return .collate(lhs <= rhs.baseExpression, rhs.collationName)
}


// MARK: - Operator > COLLATE

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func > (lhs: _SQLCollatedExpression, rhs: _SQLExpressible) -> _SQLExpression {
    return .collate(lhs.baseExpression > rhs, lhs.collationName)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func > (lhs: _SQLExpressible, rhs: _SQLCollatedExpression) -> _SQLExpression {
    return .collate(lhs > rhs.baseExpression, rhs.collationName)
}


// MARK: - Operator >= COLLATE

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func >= (lhs: _SQLCollatedExpression, rhs: _SQLExpressible) -> _SQLExpression {
    return .collate(lhs.baseExpression >= rhs, lhs.collationName)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func >= (lhs: _SQLExpressible, rhs: _SQLCollatedExpression) -> _SQLExpression {
    return .collate(lhs >= rhs.baseExpression, rhs.collationName)
}


// MARK: - Operator BETWEEN COLLATE

extension ClosedInterval where Bound: _SQLExpressible {
    /// Returns an SQL expression that compares the inclusion of a value in
    /// an interval.
    ///
    /// See https://github.com/groue/GRDB.swift/#sql-operators
    public func contains(_ element: _SQLCollatedExpression) -> _SQLExpression {
        return .collate(contains(element.baseExpression), element.collationName)
    }
}

extension HalfOpenInterval where Bound: _SQLExpressible {
    /// Returns an SQL expression that compares the inclusion of a value in
    /// an interval.
    ///
    /// See https://github.com/groue/GRDB.swift/#sql-operators
    public func contains(_ element: _SQLCollatedExpression) -> _SQLExpression {
        return (element >= start) && (element < end)
    }
}


// MARK: - Operator IN COLLATE

extension Sequence where Self.Iterator.Element: _SQLExpressible {
    /// Returns an SQL expression that compares the inclusion of a value in
    /// a sequence.
    ///
    /// See https://github.com/groue/GRDB.swift/#sql-operators
    public func contains(_ element: _SQLCollatedExpression) -> _SQLExpression {
        return .collate(contains(element.baseExpression), element.collationName)
    }
}


// MARK: - Operator IS COLLATE

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func === (lhs: _SQLCollatedExpression, rhs: _SQLExpressible?) -> _SQLExpression {
    return .collate(lhs.baseExpression === rhs, lhs.collationName)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func === (lhs: _SQLExpressible?, rhs: _SQLCollatedExpression) -> _SQLExpression {
    return .collate(lhs == rhs.baseExpression, rhs.collationName)
}


// MARK: - Operator IS NOT COLLATE

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func !== (lhs: _SQLCollatedExpression, rhs: _SQLExpressible?) -> _SQLExpression {
    return .collate(lhs.baseExpression !== rhs, lhs.collationName)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func !== (lhs: _SQLExpressible?, rhs: _SQLCollatedExpression) -> _SQLExpression {
    return .collate(lhs !== rhs.baseExpression, rhs.collationName)
}
