// MARK: - Operator =

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func == (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType?) -> _SQLExpression {
    return .equal(lhs.sqlExpression, rhs?.sqlExpression ?? .value(nil))
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func == (lhs: _SQLDerivedExpressionType, rhs: protocol<_SQLExpressionType, Boolean>?) -> _SQLExpression {
    if let rhs = rhs {
        if rhs.boolValue {
            return lhs.sqlExpression
        } else {
            return .not(lhs.sqlExpression)
        }
    } else {
        return .equal(lhs.sqlExpression, .value(nil))
    }
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func == (lhs: _SQLExpressionType?, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .equal(lhs?.sqlExpression ?? .value(nil), rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func == (lhs: protocol<_SQLExpressionType, Boolean>?, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    if let lhs = lhs {
        if lhs.boolValue {
            return rhs.sqlExpression
        } else {
            return .not(rhs.sqlExpression)
        }
    } else {
        return .equal(.value(nil), rhs.sqlExpression)
    }
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func == (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .equal(lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator !=

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func != (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType?) -> _SQLExpression {
    return .notEqual(lhs.sqlExpression, rhs?.sqlExpression ?? .value(nil))
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func != (lhs: _SQLDerivedExpressionType, rhs: protocol<_SQLExpressionType, Boolean>?) -> _SQLExpression {
    if let rhs = rhs {
        if rhs.boolValue {
            return .not(lhs.sqlExpression)
        } else {
            return lhs.sqlExpression
        }
    } else {
        return .notEqual(lhs.sqlExpression, .value(nil))
    }
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func != (lhs: _SQLExpressionType?, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .notEqual(lhs?.sqlExpression ?? .value(nil), rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func != (lhs: protocol<_SQLExpressionType, Boolean>?, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    if let lhs = lhs {
        if lhs.boolValue {
            return .not(rhs.sqlExpression)
        } else {
            return rhs.sqlExpression
        }
    } else {
        return .notEqual(.value(nil), rhs.sqlExpression)
    }
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func != (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .notEqual(lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator <

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func < (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator("<", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func < (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("<", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func < (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("<", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator <=

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func <= (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator("<=", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func <= (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("<=", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func <= (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("<=", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator >

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func > (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator(">", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func > (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator(">", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func > (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator(">", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator >=

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func >= (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator(">=", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func >= (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator(">=", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func >= (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator(">=", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator *

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func * (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator("*", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func * (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("*", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func * (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("*", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator /

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func / (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator("/", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func / (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("/", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func / (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("/", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator +

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func + (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator("+", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func + (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("+", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func + (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("+", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator - (prefix)

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public prefix func - (value: _SQLDerivedExpressionType) -> _SQLExpression {
    return .prefixOperator("-", value.sqlExpression)
}


// MARK: - Operator - (infix)

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func - (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator("-", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func - (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("-", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL arithmetic expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func - (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("-", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator AND

/// Returns an SQL logical expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func && (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator("AND", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL logical expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func && (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("AND", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL logical expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func && (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("AND", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator BETWEEN

extension Range where Element: protocol<_SQLExpressionType, BidirectionalIndex> {
    /// Returns an SQL expression that checks the inclusion of a value in
    /// a range.
    ///
    /// See https://github.com/groue/GRDB.swift/#sql-operators
    public func contains(_ element: _SQLDerivedExpressionType) -> _SQLExpression {
        return .between(value: element.sqlExpression, min: startIndex.sqlExpression, max: endIndex.predecessor().sqlExpression)
    }
}

extension ClosedInterval where Bound: _SQLExpressionType {
    /// Returns an SQL expression that checks the inclusion of a value in
    /// an interval.
    ///
    /// See https://github.com/groue/GRDB.swift/#sql-operators
    public func contains(_ element: _SQLDerivedExpressionType) -> _SQLExpression {
        return .between(value: element.sqlExpression, min: start.sqlExpression, max: end.sqlExpression)
    }
}

extension HalfOpenInterval where Bound: _SQLExpressionType {
    /// Returns an SQL expression that checks the inclusion of a value in
    /// an interval.
    ///
    /// See https://github.com/groue/GRDB.swift/#sql-operators
    public func contains(_ element: _SQLDerivedExpressionType) -> _SQLExpression {
        return (element >= start) && (element < end)
    }
}


// MARK: - Operator IN

extension Sequence where Self.Iterator.Element: _SQLExpressionType {
    /// Returns an SQL expression that checks the inclusion of a value in
    /// a sequence.
    ///
    /// See https://github.com/groue/GRDB.swift/#sql-operators
    public func contains(_ element: _SQLDerivedExpressionType) -> _SQLExpression {
        return .inOperator(map { $0.sqlExpression }, element.sqlExpression)
    }
}


// MARK: - Operator IS

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func === (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType?) -> _SQLExpression {
    return .isOperator(lhs.sqlExpression, rhs?.sqlExpression ?? .value(nil))
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func === (lhs: _SQLExpressionType?, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .isOperator(lhs?.sqlExpression ?? .value(nil), rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func === (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .isOperator(lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator IS NOT

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func !== (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType?) -> _SQLExpression {
    return .isNot(lhs.sqlExpression, rhs?.sqlExpression ?? .value(nil))
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func !== (lhs: _SQLExpressionType?, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .isNot(lhs?.sqlExpression ?? .value(nil), rhs.sqlExpression)
}

/// Returns an SQL expression that compares two values.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func !== (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .isNot(lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator OR

/// Returns an SQL logical expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func || (lhs: _SQLDerivedExpressionType, rhs: _SQLExpressionType) -> _SQLExpression {
    return .infixOperator("OR", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL logical expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func || (lhs: _SQLExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("OR", lhs.sqlExpression, rhs.sqlExpression)
}

/// Returns an SQL logical expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public func || (lhs: _SQLDerivedExpressionType, rhs: _SQLDerivedExpressionType) -> _SQLExpression {
    return .infixOperator("OR", lhs.sqlExpression, rhs.sqlExpression)
}


// MARK: - Operator NOT

/// Returns an SQL logical expression.
///
/// See https://github.com/groue/GRDB.swift/#sql-operators
public prefix func ! (value: _SQLDerivedExpressionType) -> _SQLExpression {
    return .not(value.sqlExpression)
}
