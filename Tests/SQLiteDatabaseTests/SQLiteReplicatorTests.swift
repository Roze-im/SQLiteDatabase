import XCTest
import FileLock

@testable import SQLiteDatabase

final class SQLiteReplicatorTests: XCTestCase {
    let dbPath: URL = FileManager.default.userDocumentsDirPath.appendingPathComponent("testdb.sqlite")

    lazy var logger: Logger = { print("[\($1)] \(Date()) \(String(describing: $0)) \($2)") }

    override func setUp() {
        try? FileManager.default.removeItem(at: dbPath)
    }

    func generateTestDB(at: URL) throws -> URL {
        let db = SQLiteDatabase(dbPath: at)
        let access = try db.openBlockingAccess()
        try access { db in
            try db.exec("CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT);", with: [:])
            try db.exec(
                "INSERT INTO test(id, value) VALUES (1, 'one'), (2, 'two');", with: [:]
            )
        }
        return at
    }
    // 50k records generates about 50MB of data.
    func generateGiganticTestDB(at: URL, nbRecords: Int = 50_000) throws -> URL {
        let db = SQLiteDatabase(dbPath: at)
        let access = try db.openBlockingAccess()
        var largeValue: String = ""
        for _ in 0...1000 {
            largeValue += "a"
        }
        try access { db in
            try db.exec("CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT);", with: [:])
            for i in 0..<nbRecords {
                try db.exec(
                    "INSERT INTO test(id, value) VALUES (:id, :value);",
                    with: [":id": .int64(Int64(i)),
                           ":value": .text("\(i)" + largeValue)
                          ]
                )
            }
        }
        return at
    }

    struct TestRecord: Equatable {
        let id: Int
        let value: String
    }

    func checkTestDBData(at: URL) throws {
        let db = SQLiteDatabase(dbPath: at)
        let access = try db.openBlockingAccess()
        var res = [TestRecord]()
        try access { db in
            try db.exec("SELECT id, value FROM test Order By id ;", with: [:]) {
                statement in
                guard let id: Int = try db.get(0, in: statement).value(),
                      let value: String = try db.get(1, in: statement).value()
                else {
                    XCTFail("invalid test record")
                    return
                }
                res.append(.init(id: id, value: value))
            }
        }
        XCTAssertEqual(res, [TestRecord(id: 1, value: "one"), TestRecord(id: 2, value: "two")])
    }

    func checkGiganticTestDBData(at: URL, nbRecordExpected: Int = 50_000) throws {
        let db = SQLiteDatabase(dbPath: at)
        let access = try db.openBlockingAccess()
        guard let count: Int =  try db.selectSingle("SELECT count(*) FROM Test;", withInput: [:]).value() else {
            XCTFail("couldn't select count(*)")
            return
        }

        XCTAssertEqual(count, nbRecordExpected)
    }

    func testReplicate() throws {
        let srcDB = try generateTestDB(at: dbPath)
        let replicateDB = FileManager.default.temporaryDirectory.appendingPathComponent("replicate.sqlite")
        try SQLiteReplicator.replicate(dbAt: srcDB, to: replicateDB, logger: logger)
        try checkTestDBData(
            at: replicateDB
        )
    }

    // Make sure the replicate doesn't consume too many memory,
    // or doesn't take too long
    func testReplicateMemoryConsumption() throws {
        let srcDB = try generateGiganticTestDB(at: dbPath, nbRecords: 50_000)
        let replicateDB = FileManager.default.temporaryDirectory.appendingPathComponent("replicate.sqlite")
        // Look in test result for the metric or roll over the next line
        // icon on the left, in xcode.
        measure(metrics:[XCTMemoryMetric(), XCTCPUMetric()]) {
            do {
                try SQLiteReplicator.replicate(dbAt: srcDB, to: replicateDB, logger: logger)
            } catch {
                XCTFail("replication error \(error)")
            }
        }
        try checkGiganticTestDBData(
            at: replicateDB,
            nbRecordExpected: 50_000
        )
    }
}

