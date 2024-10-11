//
//  SQLStore.swift
//  Roze
//
//  Created by Benjamin Garrigues on 17/07/2020.
//  Copyright © 2020 Roze. All rights reserved.
//

import Foundation
import SQLite3
@_exported import FileLock

/*
    Generic SQLite Database with a few helper functions.
    Capabilities should either extend or embbed this class to provide convenience functions
    for dealing with the app model.
*/

//Publicly visible function safely accessible from an Access
public protocol SQLiteQueriable: AnyObject {
    func exec(_ statement: String) throws
    func prepare(_ sql: String) throws -> OpaquePointer
    func bind(_ statement: OpaquePointer, with indexedValues: [SQLiteValue]) throws
    func bind(_ statement: OpaquePointer, with namedValues: [String: SQLiteValue]) throws
    func step(_ statement: OpaquePointer) throws -> Bool
    func get(_ iCol: Int32, in statement: OpaquePointer) throws -> SQLiteValue
    func mockUnixEpochFunction(with result: Int32) throws

    func insertOrUpdate(
        select selectRowStatement: String,
        selectBindings: [String: SQLiteValue],
        insert insertStatement: String,
        insertBindings: [String: SQLiteValue],
        update updateStatement: String,
        updateBindings: [String: SQLiteValue]?
    ) throws

    func reset(_ statement: OpaquePointer) throws
    func finalize(_ statement: OpaquePointer) throws
    func dropDB() throws

    //Unchecked methods : column type isn't checked, nor are null values.
    func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) -> Int32
    func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) -> Int64
    func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) throws -> String
    func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) -> Double
    func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) throws -> Data
}

// Create a SQLDatabase object with given file path
// then access it through either a synchronous or asynchronous access,
// using openSyncAccess or openAsyncAccess.
// Only one access can be opened at the same time on a DB.
public class SQLiteDatabase {

    //typedef not imported for swift.
    //https://stackoverflow.com/questions/26883131/sqlite-transient-undefined-in-swift
    fileprivate let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
    fileprivate let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

    enum DBError: LocalizedError {
        case unexpected(_ message: String)
        case unitialized
        case accessDisposed
        case existingAccessToDB(Access)
        case sqliteCall(errorCode: Int32?, message: String)
        case invalidResultColumn(iCol: Int32, message: String)
        case invalidNestedAccess(_ message: String)

        var errorDescription: String? {
            switch self {
            case .unexpected(let msg):
                return "unexpected SQLiteDatabase DBerror : \(msg)"
            case .unitialized:
                return "Trying to access uninitialized SQLiteDatabase"
            case .accessDisposed:
                return "Trying to disposed access to SQLiteDatabase"
            case .existingAccessToDB(let access):
                return
                    "Trying to create new access to SQliteDatabase at path \(access.db.dbPath.absoluteString) over existing one"
            case .sqliteCall(let errorCode, let message):
                return
                    "SQLiteDatabase call error (errorCode \(String(describing: errorCode)) , message : \(message))"
            case .invalidResultColumn(let iCol, let message):
                return "SQLiteDatabase invalid result column \(iCol) : \(message)"
            case .invalidNestedAccess(let message):
                return "SQLiteDatabase invalid nested access : \(message)"
            }
        }
    }

    public private(set) var dbPath: URL
    var db: OpaquePointer?

    // see https://sqlite.org/pragma.html#pragma_journal_mode
    public enum JournalMode: String {
        case delete = "DELETE"  // default, suitable for keeping sqlite contained in a single file.
        case wal = "WAL"  // suitable for concurrent read / write.

        public static let `default`: JournalMode = .delete
    }

    /// if dbPath is set to Self.inMemoryDBPath
    /// then the DB will be created as an in-memory database
    public init(dbPath: URL) {
        self.dbPath = dbPath
    }
    public static let inMemoryDBPath: URL = URL(fileURLWithPath: ":memory:")

    /// Convenience Location type that wraps the magic ":memory:" string.
    public enum Location {
        case file(URL)
        case inMemory
    }
    public convenience init(location: Location) {
        switch location {
        case .inMemory:
            self.init(dbPath: Self.inMemoryDBPath)
        case .file(let url):
            self.init(dbPath: url)
        }
    }
    var location : Location {
        if dbPath == Self.inMemoryDBPath {
            return .inMemory
        }
        return .file(dbPath)
    }

    deinit {
        closeDB()
    }

    fileprivate func fetchErrorMessage() -> String {
        guard let errorPointer = sqlite3_errmsg(db) else {
            return "no message"
        }
        return String(cString: errorPointer) + " ext err : \(sqlite3_extended_errcode(db))"
    }

    public func openDatabaseIfNeeded() throws {
        guard db == nil else { return }
        var db: OpaquePointer?
        let sqlitePath: String
        if dbPath == Self.inMemoryDBPath {
            sqlitePath = ":memory:"
        } else {
            sqlitePath = dbPath.path
        }
        guard sqlite3_open(sqlitePath, &db) == SQLITE_OK else {
            self.db = nil
            throw DBError.unexpected("could not open database at path \(dbPath)")
        }
        self.db = db
    }

    /// Careful. closing the db should first finalize prepared statements
    func closeDB() {
        guard db != nil else { return }
        sqlite3_close(db)
    }

    /// Insert Or replace changes the row-id at each "replace". This function performs a real
    /// "insert or update" by first selecting the row to update, and fallback to an insert otherwise
    /// - Parameters:
    ///   - select : the  select statement if this statement returns anything, the update statement will be performed. Otherwise the
    ///   - selectBindings: bindings for select statement
    ///   - insert: the insert statement
    ///   - insertBindings: bindings for insert statement
    ///   - update: the update statement
    ///   - updateBindings: bindings for update statement (optional: if nil insertBindings will be used)
    public func insertOrUpdate(
        select selectRowStatement: String,
        selectBindings: [String: SQLiteValue],
        insert insertStatement: String,
        insertBindings: [String: SQLiteValue],
        update updateStatement: String,
        updateBindings: [String: SQLiteValue]? = nil
    ) throws {
        var found: Bool = false
        try exec(
            selectRowStatement,
            with: selectBindings,
            processResultRows: { statement in
                found = true
            })
        if found {
            try exec(updateStatement, with: updateBindings ?? insertBindings)
        } else {
            try exec(insertStatement, with: insertBindings)
        }
    }

    public func exec(_ statement: String) throws {
        guard let db = db else {
            throw DBError.unitialized
        }
        let res = sqlite3_exec(db, statement, nil, nil, nil)
        guard res == SQLITE_OK else {
            throw DBError.sqliteCall(errorCode: res, message: fetchErrorMessage())
        }
    }

    public func prepare(_ sql: String) throws -> OpaquePointer {
        guard let db = db else {
            throw DBError.unitialized
        }

        var statement: OpaquePointer?
        let res = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
        guard res == SQLITE_OK else {
            throw DBError.sqliteCall(errorCode: res, message: fetchErrorMessage())
        }
        guard let statementNonNil = statement else {
            throw DBError.unexpected("nil statement")
        }
        return statementNonNil
    }

    public func bind(_ statement: OpaquePointer, with indexedValues: [SQLiteValue]) throws {
        for (i, v) in indexedValues.enumerated() {
            let res = bind(statement, value: v, at: Int32(i + 1))
            if res != SQLITE_OK {
                sqlite3_reset(statement)
                throw DBError.sqliteCall(
                    errorCode: res, message: "error binding value \(v) in statement")
            }
        }
    }

    public func bind(_ statement: OpaquePointer, with namedValues: [String: SQLiteValue]) throws {
        var indexedValues = (0..<namedValues.count).map { (_) in SQLiteValue.null }
        for (name, value) in namedValues {
            let pos = sqlite3_bind_parameter_index(statement, name)
            guard pos > 0 else {
                print("could not find parameter \(name) to bind")
                continue
            }
            indexedValues[Int(pos) - 1] = value
        }
        try bind(statement, with: indexedValues)
    }

    @inline(__always)
    fileprivate func bind(_ statement: OpaquePointer, value: SQLiteValue, at index: Int32) -> Int32
    {
        switch value {
        case .null:
            return sqlite3_bind_null(statement, index)
        case .int32(let value):
            return sqlite3_bind_int(statement, index, value)
        case .int64(let value):
            return sqlite3_bind_int64(statement, index, value)
        case .double(let value):
            return sqlite3_bind_double(statement, index, value)
        case .blob(let value):
            return value.withUnsafeBytes { (buffer) -> Int32 in
                return sqlite3_bind_blob(
                    statement,
                    index,
                    buffer.baseAddress,
                    Int32(buffer.count),
                    SQLITE_TRANSIENT)
            }
        case .text(let value):
            let stringBytes = value.utf8CString

            return stringBytes.withUnsafeBufferPointer { (buffer) -> Int32 in
                return sqlite3_bind_text(
                    statement,
                    index,
                    buffer.baseAddress,
                    Int32(buffer.count - 1),  // buffer.count includes null termination (\0)
                    SQLITE_TRANSIENT)
            }
        }

    }

    //"Stepping" statement means executing it, and loading the result
    //Returns whether new results are availabe or not.
    public func step(_ statement: OpaquePointer) throws -> Bool {
        let stepRes = sqlite3_step(statement)
        switch stepRes {
        case SQLITE_ROW:
            return true
        case SQLITE_DONE:
            return false
        default:
            let message = String(cString: sqlite3_errmsg(db))
            throw DBError.sqliteCall(
                errorCode: stepRes,
                message: "sqlite3_step error \(stepRes) \(message)"
            )
        }
    }

    // MARK: - Result getter.
    //Note : get -> swift type functions expect non-null results.
    public func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) -> Int32 {
        return sqlite3_column_int(statement, iCol)
    }
    public func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) -> Int64 {
        return sqlite3_column_int64(statement, iCol)
    }
    public func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) throws -> String {
        guard let cString = sqlite3_column_text(statement, iCol) else {
            throw DBError.invalidResultColumn(iCol: iCol, message: "sqlite3_column_text nil result")
        }
        return String(cString: cString)
    }
    public func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) -> Double {
        return sqlite3_column_double(statement, iCol)
    }
    public func getUnchecked(_ iCol: Int32, in statement: OpaquePointer) throws -> Data {
        guard let dataPtr = sqlite3_column_blob(statement, iCol) else {
            throw DBError.invalidResultColumn(iCol: iCol, message: "sqlite3_column_blob nil result")
        }
        let dataBytes = sqlite3_column_bytes(statement, iCol)
        return Data(bytes: dataPtr, count: Int(dataBytes))
    }

    public func get(_ iCol: Int32, in statement: OpaquePointer) throws -> SQLiteValue {
        let colType = sqlite3_column_type(statement, iCol)
        switch colType {
        case SQLITE_INTEGER:
            return .int64(getUnchecked(iCol, in: statement))
        case SQLITE_FLOAT:
            return .double(getUnchecked(iCol, in: statement))
        case SQLITE_TEXT:
            return try .text(getUnchecked(iCol, in: statement))
        case SQLITE_BLOB:
            return try .blob(getUnchecked(iCol, in: statement))
        case SQLITE_NULL:
            return .null
        default:
            throw DBError.unexpected("unhandled column type : \(colType) at index \(iCol)")
        }
    }

    public func reset(_ statement: OpaquePointer) throws {
        let res = sqlite3_reset(statement)
        guard res == SQLITE_OK else {
            throw DBError.sqliteCall(errorCode: res, message: "sqlite3_reset error")
        }
    }

    public func finalize(_ statement: OpaquePointer) throws {
        let res = sqlite3_finalize(statement)
        guard res == SQLITE_OK else {
            throw DBError.sqliteCall(errorCode: res, message: "sqlite3_finalize error")
        }
    }

    public func dropDB() throws {
        sqlite3_close(db)
        try FileManager.default.removeItem(at: dbPath)
        activeAccess = nil
        db = nil
    }

    //current registerd access (only one allowed). Variable protected by dispatch queue.
    fileprivate var activeAccessSynchronizationQueue: DispatchQueue = DispatchQueue(
        label: "sqlitedb.activeaccess.\(UUID())", qos: .userInteractive)
    fileprivate weak var activeAccess: Access?
    fileprivate func safeUpdateActiveAccess(update: () throws -> Bool) throws -> Bool {
        try activeAccessSynchronizationQueue.sync {
            return try update()
        }
    }
    fileprivate func registerAccess(_ access: Access) throws {
        _ = try safeUpdateActiveAccess {
            if let activeAccess = activeAccess {
                throw DBError.existingAccessToDB(activeAccess)
            }
            self.activeAccess = access
            return true
        }
    }
    //returns whether the access was the one registered as the active one
    fileprivate func unregisterAccess(_ access: Access) -> Bool {
        let res = try? safeUpdateActiveAccess {
            guard activeAccess === access else { return false }
            activeAccess = nil
            return true
        }
        return res ?? false
    }

    public func openBlockingAccess(
        queue: DispatchQueue = DispatchQueue.main, journalMode: JournalMode? = nil,
        busyTimeoutMS: Int? = nil
    ) throws -> Access.Blocking {
        return try .init(
            db: self, queue: queue, journalMode: journalMode, busyTimeoutMS: busyTimeoutMS)
    }

    public func openNonBlockingAccess(
        queue: DispatchQueue = DispatchQueue(
            label: "roze.sqlitedb.queue",
            qos: .userInitiated),
        journalMode: JournalMode? = nil
    ) throws -> Access.NonBlocking {
        return try .init(db: self, queue: queue, journalMode: journalMode)
    }

    // MARK: mock unix epoch
    /// Custom function creation
    /// *** HIGHLY EXPERIMENTAL ***. FOR UNIT TESTS ONLY !!!
    fileprivate class Wrapper<T> {
        let val: T
        init(val: T) {
            self.val = val
        }
    }
    fileprivate var mockedUnixEpochWrapper: Wrapper<Int32>?
    public func mockUnixEpochFunction(with value: Int32) throws {
        guard let db = db else {
            throw DBError.unitialized
        }
        // create an object that will wrap the value
        let w = Wrapper(val: value)
        self.mockedUnixEpochWrapper = w

        sqlite3_create_function(
            db,
            "UNIXEPOCH".cString(using: .utf8),
            0,
            SQLITE_UTF8,
            Unmanaged.passRetained(w).toOpaque(),
            { context, argc, arguments in
                guard let valuePtr = sqlite3_user_data(context) else {
                    return sqlite3_result_int(context, -1)
                }
                let wrapper = Unmanaged<Wrapper<Int32>>.fromOpaque(valuePtr).takeRetainedValue()
                return sqlite3_result_int(context, wrapper.val)
            },
            nil,
            /* xFinal */ nil)
    }
    /** END OF CUSTOM FUNCTION CREATION **/

    // swiftlint:disable nesting
    public class Access {
        let queue: DispatchQueue
        public let db: SQLiteDatabase
        private(set) public var disposed: Bool = false
        init(
            db: SQLiteDatabase, queue: DispatchQueue, journalMode: JournalMode? = nil,
            busyTimeoutMS: Int? = nil
        ) throws {
            self.db = db
            self.queue = queue
            try db.openDatabaseIfNeeded()
            try db.registerAccess(self)
            if let journalMode = journalMode {
                let journal: String? = try db.selectSingle(
                    "PRAGMA journal_mode = \(journalMode.rawValue)", withInput: [:]
                ).value()
                if journal?.lowercased() != journalMode.rawValue.lowercased() {
                    throw DBError.unexpected("couldn't set journal mode to \(journalMode)")
                }
            }
            if let busyTimeoutMS = busyTimeoutMS {
                let busyTimeoutResponse: Int? = try db.selectSingle(
                    "PRAGMA busy_timeout = \(busyTimeoutMS)", withInput: [:]
                ).value()
                if busyTimeoutResponse != busyTimeoutMS {
                    throw DBError.unexpected(
                        "couldn't set busy timeout to \(busyTimeoutMS): value \(String(describing: busyTimeoutResponse))"
                    )
                }
            }
        }
        deinit {
            dispose()
        }

        public func dispose() {
            guard !disposed else { return }
            _ = db.unregisterAccess(self)
            disposed = true
        }

        public class Blocking: Access {
            public func callAsFunction<T>(
                parentAccess: SQLiteQueriable? = nil,
                _ perform: (_ queryable: SQLiteQueriable) throws -> T
            ) throws -> T {
                guard !disposed else {
                    throw DBError.accessDisposed
                }
                if let parentAccess = parentAccess {
                    guard parentAccess === db else {
                        assertionFailure("cross db nested access not supported")
                        throw DBError.invalidNestedAccess(
                            "cross db nested transaction not supported")
                    }
                    return try perform(parentAccess)
                }
                if Thread.isMainThread && queue == DispatchQueue.main {
                    return try perform(self.db)
                } else {
                    return try queue.sync {
                        return try perform(self.db)
                    }
                }
            }
        }

        public class NonBlocking: Access {
            public func callAsFunction(
                parentAccess: SQLiteQueriable? = nil,
                _ perform: @escaping (_ queryable: SQLiteQueriable) -> Void
            ) {
                guard !disposed else {
                    print("calling disposed non-blocking access !")
                    return
                }
                if let parentAccess = parentAccess {
                    guard parentAccess === db else {
                        assertionFailure("cross db nested transaction not supported")
                        return
                    }
                    perform(parentAccess)
                }
                queue.async {
                    perform(self.db)
                }
            }
        }
    }
}

extension SQLiteDatabase: SQLiteQueriable {}
