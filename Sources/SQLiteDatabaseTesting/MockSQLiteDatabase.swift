//
//  File.swift
//
//
//  Created by Benjamin Garrigues on 21/10/2023.
//

import Foundation
import SQLiteDatabase

extension SQLiteDatabase {
  public static func mockInMemory() -> SQLiteDatabase {
    return SQLiteDatabase(
      dbPath: SQLiteDatabase.inMemoryDBPath
    )
  }
}
