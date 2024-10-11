//
//  SQliteValue.swift
//  Roze
//
//  Created by Benjamin Garrigues on 18/07/2020.
//  Copyright © 2020 Roze. All rights reserved.
//

import Foundation

public enum SQLiteValue {
    case blob(Data)
    case double(Double)
    case int32(Int32)
    case int64(Int64)
    case null
    case text(String)

    // MARK: - Swift / SQlite types convenience converters
    // MARK: Int
    public func value() -> Int? {
        switch self {
        case .int32(let val):
            return Int(val)
        case .int64(let val):
            return Int(val)
        default:
            return nil
        }
    }

    public init(_ i: Int?) {
        guard let i = i else {
            self = .null
            return
        }
        self = .int64(Int64(i))
    }

    // MARK: String
    public func value() -> String? {
        switch self {
        case .text(let val):
            return val
        default:
            return nil
        }
    }
    public init(_ s: String?) {
        guard let s = s else {
            self = .null
            return
        }
        self = .text(String(s))
    }

    // MARK: Double
    public func value() -> Double? {
        switch self {
        case .double(let val):
            return val
        default:
            return nil
        }
    }
    public init(_ f: Double?) {
        guard let f = f else {
            self = .null
            return
        }
        self = .double(f)
    }

    // MARK: Data
    public func value() -> Data? {
        switch self {
        case .blob(let val):
            return val
        default:
            return nil
        }
    }
    public init(_ d: Data?) {
        guard let d = d else {
            self = .null
            return
        }
        self = .blob(d)
    }
}
