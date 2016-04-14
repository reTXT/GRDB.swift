import Foundation

/// NSDate are stored in the database using the format
/// "yyyy-MM-dd HH:mm:ss.SSS", in the UTC time zone.
extension NSDate : DatabaseValueConvertible {
    
    /// Returns a value that can be stored in the database.
    public var databaseValue: DatabaseValue {
        return storageDateFormatter.string(from: self).databaseValue
    }
    
    /// Returns an NSDate initialized from *databaseValue*, if possible.
    public static func from(databaseValue: DatabaseValue) -> Self? {
        if let databaseDateComponents = DatabaseDateComponents.from(databaseValue: databaseValue) {
            return from(databaseDateComponents: databaseDateComponents)
        }
        if let julianDayNumber = Double.from(databaseValue: databaseValue) {
            return from(julianDayNumber: julianDayNumber)
        }
        return nil
    }
    
    private static func from(julianDayNumber: Double) -> Self? {
        // Conversion uses the same algorithm as SQLite: https://www.sqlite.org/src/artifact/8ec787fed4929d8c
        let JD = Int64(julianDayNumber * 86400000)
        let Z = Int(((JD + 43200000)/86400000))
        var A = Int(((Double(Z) - 1867216.25)/36524.25))
        A = Z + 1 + A - (A/4)
        let B = A + 1524
        let C = Int(((Double(B) - 122.1)/365.25))
        let D = (36525*(C&32767))/100
        let E = Int((Double(B-D)/30.6001))
        let X1 = Int((30.6001*Double(E)))
        let day = B - D - X1
        let month = E<14 ? E-1 : E-13
        let year = month>2 ? C - 4716 : C - 4715
        var s = Int(((JD + 43200000) % 86400000))
        var second = Double(s)/1000.0
        s = Int(second)
        second -= Double(s)
        let hour = s/3600
        s -= hour*3600
        let minute = s/60
        second += Double(s - minute*60)
        
        let dateComponents = NSDateComponents()
        dateComponents.year = year
        dateComponents.month = month
        dateComponents.day = day
        dateComponents.hour = hour
        dateComponents.minute = minute
        dateComponents.second = Int(second)
        dateComponents.nanosecond = Int((second - Double(Int(second))) * 1.0e9)
        
        let date = UTCCalendar.date(from: dateComponents)!
        return self.init(timeInterval: 0, since: date)
    }
    
    private static func from(databaseDateComponents: DatabaseDateComponents) -> Self? {
        guard databaseDateComponents.format.hasYMDComponents else {
            // Refuse to turn hours without any date information into NSDate:
            return nil
        }
        let date = UTCCalendar.date(from: databaseDateComponents.dateComponents)!
        return self.init(timeInterval: 0, since: date)
    }
}

/// The DatabaseDate date formatter for stored dates.
private let storageDateFormatter: NSDateFormatter = {
    let formatter = NSDateFormatter()
    formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    formatter.locale = NSLocale(localeIdentifier: "en_US_POSIX")
    formatter.timeZone = NSTimeZone(forSecondsFromGMT: 0)
    return formatter
    }()

// The NSCalendar for stored dates.
private let UTCCalendar: NSCalendar = {
    let calendar = NSCalendar(calendarIdentifier: NSCalendarIdentifierGregorian)!
    calendar.locale = NSLocale(localeIdentifier: "en_US_POSIX")
    calendar.timeZone = NSTimeZone(forSecondsFromGMT: 0)
    return calendar
    }()
