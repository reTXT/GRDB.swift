- [ ] Swift 3
    - [X] lowercased enum cases
    - [X] XXXType protocol: remove Type. Compare with Sequence vs IteratorProtocol 
    - [X] Compare FetchedRecordsController.record(at:NSIndexPath) with NSFetchedResultsController
    - [ ] add(function:), add(collation:), add(transactionObserver:)
    - [ ] make- for factories
    - [ ] is- for flags
    - [ ] -ed for methods that return a variant
    - [ ] FetchedRecordsController: configure(_ cell: UITableViewCell, at indexPath: NSIndexPath)
    - [ ] Not sure: row.value(at: 0) (used to be row.value(atIndex: 0))
    - [ ] lowercase(d), uppercase(d), capitalize(d), localizedLowercase(d), etc: pick one form, and pick var or func.  
- [ ] API diff
- [ ] What is the SQLITE_OPEN_WAL open flag?
- [ ] Read https://github.com/ccgus/fmdb/issues/262 and understand https://lists.apple.com/archives/cocoa-dev/2012/Aug/msg00527.html
- [ ] FetchedRecordsController needs a property that disables changes computation and calls to delegate.controller(_:didChangeRecord:withEvent:).
- [ ] DatabaseValue.failableValue() is not a nice name.
- [ ] Support for resource values (see https://developer.apple.com/library/ios/qa/qa1719/_index.html)
- [ ] DOC: Since commit e6010e334abdf98eb9f62c1d6abbb2a9e8cd7d19, one can not use the raw SQLite API without importing the SQLite module for the platform. We need to document that.
- [ ] Query builder
    - [ ] SELECT readers.*, books.* FROM ... JOIN ...
    - [ ] date functions
    - [ ] NOW
    - [ ] RANDOM() https://www.sqlite.org/lang_corefunc.html
    - [ ] LIKE https://www.sqlite.org/lang_expr.html
    - [ ] GLOB https://www.sqlite.org/lang_expr.html
    - [ ] MATCH https://www.sqlite.org/lang_expr.html
    - [ ] REGEXP https://www.sqlite.org/lang_expr.html
    - [ ] CASE x WHEN w1 THEN r1 WHEN w2 THEN r2 ELSE r3 END https://www.sqlite.org/lang_expr.html

Not sure:

- [ ] Make FetchRequest adopt Equatable
- [ ] Turn DatabaseWriter.readFromWrite to Database.readFromCurrentState { ... } ?
- [ ] Refactor errors in a single type?
- [ ] Since Records' primary key are infered, no operation is possible on the primary key unless we have a Database instance. It's impossible to define the record.primaryKey property, or to provide a copy() function that does not clone the primary key: they miss the database that is the only object aware of the primary key. Should we change our mind, and have Record explicitly expose their primary key again?
- [ ] Have Record adopt Hashable and Equatable, based on primary key. Problem: we can't do it know because we don't know the primary key until we have a database connection.


Require changes in the Swift language:

- [ ] Specific and optimized Optional<StatementColumnConvertible>.fetch... methods when http://openradar.appspot.com/22852669 is fixed.


Requires recompilation of SQLite:

- [ ] https://www.sqlite.org/c3ref/column_database_name.html could help extracting out of a row a subrow only made of columns that come from a specific table. Requires SQLITE_ENABLE_COLUMN_METADATA which is not set on the sqlite3 lib that ships with OSX.



Reading list:

- VACUUM (https://blogs.gnome.org/jnelson/)
- Full text search (https://www.sqlite.org/fts3.html. Related: https://blogs.gnome.org/jnelson/)
- https://www.sqlite.org/undoredo.html
- http://www.sqlite.org/intern-v-extern-blob.html
- List of documentation keywords: https://swift.org/documentation/api-design-guidelines.html#special-instructions
- https://www.zetetic.net/sqlcipher/
- https://sqlite.org/sharedcache.html
