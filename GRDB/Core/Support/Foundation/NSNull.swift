import Foundation

/// NSNull adopts DatabaseValueConvertible
extension NSNull: DatabaseValueConvertible {
    
    /// Returns DatabaseValue.null.
    public var databaseValue: DatabaseValue {
        return .null
    }
    
    /// Returns nil.
    public static func from(databaseValue: DatabaseValue) -> Self? {
        return nil
    }
}
