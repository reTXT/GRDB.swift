import Foundation

/// NSString adopts DatabaseValueConvertible
extension NSString: DatabaseValueConvertible {
    
    /// Returns a value that can be stored in the database.
    public var databaseValue: DatabaseValue {
        return (self as String).databaseValue
    }
    
    /// Returns an NSString initialized from *databaseValue*, if possible.
    public static func from(databaseValue: DatabaseValue) -> Self? {
        guard let string = String.from(databaseValue: databaseValue) else {
            return nil
        }
        return self.init(string: string)
    }
}
