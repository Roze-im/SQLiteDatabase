//
//  SQLiteQueryable+roze.swift
//  Roze
//
//  Created by Benjamin Garrigues on 18/07/2020.
//  Copyright © 2020 Roze. All rights reserved.
//

import Foundation

// Convenience helper functions over the standard interface
extension SQLiteQueriable {
    //Convenience exec for one-shot statements
    public func exec(
        _ sql: String,
        with namedValues: [String: SQLiteValue],
        processResultRows: ((_ statement: OpaquePointer) throws -> Void)? = nil
    ) throws {
        let statement = try prepare(sql)
        defer {
            _ = try? finalize(statement)
        }
        try bind(statement, with: namedValues)
        while try step(statement) {
            try processResultRows?(statement)
        }
    }

    //Convenience exec for prepared statements
    public func exec(
        _ statement: OpaquePointer,
        with namedValues: [String: SQLiteValue],
        processResultRows: ((_ statement: OpaquePointer) throws -> Void)? = nil
    ) throws {
        defer {
            _ = try? reset(statement)
        }
        try bind(statement, with: namedValues)
        while try step(statement) {
            try processResultRows?(statement)
        }
    }

    public func selectSingle(_ sql: String, withInput input: [String: SQLiteValue]) throws
        -> SQLiteValue
    {
        var res: SQLiteValue = .null
        try exec(sql, with: input) { (statement) in
            res = try self.get(0, in: statement)
        }
        return res
    }

    public func bulkExec(_ sql: String, withInput input: [[String: SQLiteValue]]) throws {

        let stmt = try prepare(sql)
        defer {
            _ = try? finalize(stmt)
        }
        try exec("BEGIN TRANSACTION")
        for i in input {
            try bind(stmt, with: i)
            _ = try step(stmt)
            try reset(stmt)
        }
        try exec("COMMIT TRANSACTION")
    }

    public func bulkExec(
        _ statement: OpaquePointer,
        withInput input: [[String: SQLiteValue]],
        wrapInTransaction: Bool = true
    ) throws {

        defer {
            _ = try? reset(statement)
        }
        if wrapInTransaction {
            try exec("BEGIN TRANSACTION;")
        }
        do {
            for i in input {
                try bind(statement, with: i)
                _ = try step(statement)
                try reset(statement)
            }
        } catch {
            if wrapInTransaction {
                try exec("ROLLBACK;")
            }
            throw error
        }
        if wrapInTransaction {
            try exec("COMMIT;")
        }
    }
}
