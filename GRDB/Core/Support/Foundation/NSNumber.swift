import Foundation

/// NSNumber adopts DatabaseValueConvertible
extension NSNumber: DatabaseValueConvertible {
    
    /// Returns a value that can be stored in the database.
    public var databaseValue: DatabaseValue {
        switch String(validatingUTF8: objCType)! {
        case "c",
             "C",
             "s",
             "S",
             "i",
             "I",
             "l",
             "L",
             "q",
             "Q":
            return int64Value.databaseValue
        case "f",
             "d":
            return doubleValue.databaseValue
        case "B":
            return boolValue.databaseValue
        case let objCType:
            fatalError("DatabaseValueConvertible: Unsupported NSNumber type: \(objCType)")
        }
    }
    
    /// Returns an NSNumber initialized from *databaseValue*, if possible.
    public static func from(databaseValue: DatabaseValue) -> Self? {
        switch databaseValue.storage {
        case .Int64(let int64):
            return self.init(value: int64)
        case .Double(let double):
            return self.init(value: double)
        default:
            return nil
        }
    }
}
