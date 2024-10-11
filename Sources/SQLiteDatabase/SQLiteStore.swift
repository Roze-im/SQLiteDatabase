//
//  SQLiteStore.swift
//  RozeEngine-Swift
//
//  Created by Thibaud David on 27/01/2021.
//

import Foundation
import FileLock

public protocol SQLiteStore: AnyObject {
    var dbFilename: String { get }

    var storeLogger: Logger { get }
    var dbAccess: SQLiteDatabase.Access.Blocking { get set }
    var dbPath: URL { get }
    var dispatchQueue: DispatchQueue { get }

    init(logger: @escaping Logger, rootPath: URL) throws
    static func createSchema(db: SQLiteQueriable) throws
    func recreatePreparedStatements(db: SQLiteQueriable) throws
    func dropPreparedStatements(db: SQLiteQueriable) throws
}

public extension SQLiteStore {
    static func defaultRootPath() -> URL {
        return FileManager.default.userDocumentsDirPath
    }

    func resetDB() throws {
        try dbAccess {
            try dropPreparedStatements(db: $0)
            try $0.dropDB()
        }
        dbAccess.dispose()
        try openAccess()
    }

    func openAccess() throws {
        guard dbAccess.disposed else {
            storeLogger(self, .error, "trying to open message store with active access. ignoring")
            return
        }
        let db = SQLiteDatabase(dbPath: dbPath)
        dbAccess = try db.openBlockingAccess(queue: dispatchQueue)
        try dbAccess {
            try Self.createSchema(db: $0)
            try recreatePreparedStatements(db: $0)
        }
    }

    func recreatePreparedStatements(db: SQLiteQueriable) throws {
        storeLogger(self, .debug, "No prepared sstatements to create")
    }

    func dropDb() throws {
        try dbAccess {
            try $0.dropDB()
        }
        dbAccess.dispose()
    }

    func performInSingleDbAccess(
        parentAccess: SQLiteQueriable? = nil,
        closure: (SQLiteQueriable) throws -> Void
    ) throws {
        return try dbAccess(parentAccess: parentAccess) { db in
            try closure(db)
        }
    }
}

extension FileManager {
    var userDocumentsDirPath: URL {
        #if targetEnvironment(macCatalyst)
        return applicationSupportDirPath
        #else
        return FileManager.default.urls(
            for: FileManager.SearchPathDirectory.documentDirectory,
            in: FileManager.SearchPathDomainMask.userDomainMask
        )[0]
        #endif
    }

    private var applicationSupportDirPath: URL {
        FileManager.default.urls(
            for: FileManager.SearchPathDirectory.applicationSupportDirectory,
            in: FileManager.SearchPathDomainMask.userDomainMask
        )[0]
    }
}
