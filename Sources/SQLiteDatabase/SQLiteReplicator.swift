//
//  File.swift
//
//
//  Created by Benjamin Garrigues on 17/10/2023.
//

import FileLock
import Foundation
import Debouncer

public class SQLiteReplicator {

    var logger: Logger
    var sourceDB: SQLiteDatabase.Access.Blocking
    var targetDBPath: URL
    public let throttler: Throttler
    let fileLocker: FileLock
    public let defaultThrottlingInterval: TimeInterval

    public init(
        logger: @escaping Logger,
        sourceDB: SQLiteDatabase.Access.Blocking,
        targetDBPath: URL,
        throttleInterval: TimeInterval = 0.3,
        replicationQueueQoS: DispatchQoS = .userInitiated,
        replicationQueueTarget: DispatchQueue = .global(qos: .userInitiated)
    ) {
        self.logger = logger
        self.sourceDB = sourceDB
        self.targetDBPath = targetDBPath
        let processingQueue = DispatchQueue(
            label: "sqlitereplicator",
            qos: replicationQueueQoS,
            target: replicationQueueTarget
        )
        self.defaultThrottlingInterval = throttleInterval
        self.throttler = Throttler(
            queue: processingQueue
        )
        self.fileLocker = .init(
            filePath: targetDBPath,
            lockType: .delete,
            logger: logger
        )
    }

    /// Blocking call
    public func performReplication() throws {

        logger(
            self, .trace, "performing replication to \(targetDBPath)"
        )
        // Don't lock the db access queue waiting for the file lock to be obtained, it may take a while.
        // Warning: this means we can deadlock if a task in the same db access queue tries to obtain
        // that same file lock (but there's no reason for that to happen).
        // aka -> always perform the locking in the same order: file first, then access, and everything should be fine.
        try fileLocker.performInLock(debugInfos: "replication to \(targetDBPath.path)") {
            logger(
                self, .trace, "lock obtained to \(targetDBPath.path)"
            )

            try sourceDB { db in
                logger(
                    self, .trace,
                    "source DB access obtained. Try removing file at \(targetDBPath.path)"
                )

                // delete file and ignore errors.
                _ = try? FileManager.default.removeItem(at: targetDBPath)
                // perform the replicate. VACUUM INTO is the recommended method.
                // (Supposed to) work with any journaling mode (WAL, etc), as opposed to just copying file(s), which is error prone
                logger(
                    self, .trace, "EXEC VACUUM INTO \(targetDBPath.path)"
                )
                try db.exec("VACUUM INTO '\(targetDBPath.path)';")
                logger(
                    self, .trace, "DONE EXEC VACUUM INTO \(targetDBPath.path)"
                )

            }
        }
        logger(
            self, .trace, "DONE performing replication to \(targetDBPath.path)"
        )
    }

    /// Non-blocking , throttled call
    public func scheduleReplication(throttlingInterval: TimeInterval? = nil) {
        let ti = throttlingInterval ?? defaultThrottlingInterval

        logger(
            self, .trace, "scheduleReplication (throttlingInterval \(ti))"
        )

        throttler.throttle(with: ti) { [weak self] in
            guard let self else { return }
            do {
                try performReplication()
            } catch {
                self.logger(self, .error, "replication to \(targetDBPath) failed : \(error)")
            }
        }
    }

    static public func replicate(
        db: SQLiteDatabase.Access.Blocking,
        to: URL,
        logger: @escaping Logger
    ) throws {
        let replicator = SQLiteReplicator(
            logger: logger,
            sourceDB: db,
            targetDBPath: to
        )
        try replicator.performReplication()
    }

    /// WARNING : make sure the current process doesn't already have an open connexion to the db.
    /// SQLite is NOT multi-threaded.
    static public func replicate(
        dbAt sourceURL: URL,
        to: URL,
        logger: @escaping Logger,
        lockSourceDB: Bool = true
    ) throws {

        if lockSourceDB {
            let fileLock = FileLock(
                filePath: sourceURL,
                lockType: .read,
                logger: logger
            )
            // db connection needs to be performed inside the lock. Otherwise it could lead to a race
            // between the time you've connected and the time you've entered the lock.
            // Resulting in a sqlite I/O error 10
            let db = SQLiteDatabase(dbPath: sourceURL)
            let access = try db.openBlockingAccess()
            try fileLock.performInLock(debugInfos: "\(sourceURL.path) access lock") {
                try replicate(db: access, to: to, logger: logger)
            }
            logger(self, .trace, "exited lock")
        } else {
            let db = SQLiteDatabase(dbPath: sourceURL)
            let access = try db.openBlockingAccess()
            try replicate(db: access, to: to, logger: logger)
        }
    }
}
