import XCTest
@testable import SQLiteDatabase

final class SQLiteDatabaseTests: XCTestCase {
    let dbPath: URL = FileManager.default.userDocumentsDirPath.appendingPathComponent("testdb.sqlite")

    override func setUp() {
        try? FileManager.default.removeItem(at: dbPath)
    }

    func testMockUNIXEpoch() throws {
        let db = SQLiteDatabase(dbPath: dbPath)
        let dbAccess = try db.openBlockingAccess()
        try dbAccess { db in
            try db.mockUnixEpochFunction(with: 42)
            let value = try db.selectSingle("SELECT UNIXEPOCH()", withInput: [:])
            switch value {
            case .int64(let epoch):
                XCTAssertEqual(epoch, 42)
            default:
                XCTFail("unexpected value : \(value)")
            }
        }
    }
}
