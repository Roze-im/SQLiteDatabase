//
//  File.swift
//
//
//  Created by Benjamin Garrigues on 30/08/2023.
//

import Foundation
import FileLock

public protocol PreparedStatementSet {
  var allStatements: [OpaquePointer] { get }
}

/// Default implementation for SQLiteStore components
open class SQLiteStoreBase<PreparedStatements: PreparedStatementSet>: SQLiteStore {

  public enum StoreError: Error {
    case invalidColumnType(String)
    case preparedStatementsNotInitialized
  }

  public var dbFilename: String

  public var storeLogger: Logger
  public var dbAccess: SQLiteDatabase.Access.Blocking
  public var dbPath: URL
  public private(set) var dispatchQueue: DispatchQueue
  public var statements: PreparedStatements?

  public var queueName: String

  public required convenience init(logger: @escaping Logger, rootPath: URL) throws {
    try self.init(
      logger: logger,
      rootPath: rootPath,
      dbFileName: "\(Self.self).sqlite",
      accessQueueName: "\(Self.self)_access_queue",
      accessQueueQoS: DispatchQoS.userInitiated)
  }
  public init(
    logger: @escaping Logger,
    rootPath: URL,
    dbFileName: String,
    accessQueueName queueName: String,
    accessQueueQoS queueQoS: DispatchQoS,
    journalMode: SQLiteDatabase.JournalMode? = nil,
    busyTimeoutMS: Int? = nil
  ) throws {
    self.storeLogger = logger
    self.queueName = queueName
    dbFilename = dbFileName
    dbPath = rootPath.appendingPathComponent(dbFilename)
    dispatchQueue = DispatchQueue(
      label: queueName,
      qos: queueQoS
    )
    let db = SQLiteDatabase(dbPath: dbPath)
    dbAccess = try db.openBlockingAccess(
      queue: dispatchQueue, journalMode: journalMode, busyTimeoutMS: busyTimeoutMS
    )

    statements = try dbAccess {
      try Self.createSchema(db: $0)
      return try Self.generateStatements(db: $0, logger: logger)
    }
  }

  /// Per https://www.sqlite.org/c3ref/close.html
  /// unfinalized prepared statements will keep the sqlite connection open.
  deinit {
    do {
      try dbAccess {
        try dropPreparedStatements(db: $0)
      }
    } catch {
        storeLogger(self, .error, "\(dbFilename) could not dropPreparedStatements at deinit : \(error)")
    }
  }

  open class func generateStatements(
    db: SQLiteQueriable, logger: Logger
  ) throws -> PreparedStatements {
    fatalError("not implemented")
  }

  open class func createSchema(db: SQLiteQueriable) throws {
    fatalError("not implemented")
  }

  public func dropPreparedStatements(db: SQLiteQueriable) throws {
    try statements?.allStatements.forEach {
      try db.finalize($0)
    }
  }
}
