// swift-tools-version: 5.6
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
  name: "SQLiteDatabase",
  platforms: [
    .macOS(.v10_15),
    .iOS(.v15),
    .macCatalyst(.v15),
  ],

  products: [
    // Products define the executables and libraries a package produces, and make them visible to other packages.
    .library(
      name: "SQLiteDatabase",
      targets: ["SQLiteDatabase"]),
    .library(
      name: "SQLiteDatabaseTesting",
      targets: ["SQLiteDatabaseTesting"])
  ],
  dependencies: [
    // Dependencies declare other packages that this package depends on.
    // .package(url: /* package url */, from: "1.0.0"),
    .package(url: "https://github.com/Roze-im/FileLock.git", branch: "master"),
    .package(url: "https://github.com/Roze-im/Debouncer.git", branch: "master"),
  ],
  targets: [
    // Targets are the basic building blocks of a package. A target can define a module or a test suite.
    // Targets can depend on other targets in this package, and on products in packages this package depends on.
    .target(
      name: "SQLiteDatabase",
      dependencies: [
        .byName(name: "FileLock"),
        .byName(name: "Debouncer"),
      ]),
    .target(
      name: "SQLiteDatabaseTesting",
      dependencies: [
        .byName(name: "SQLiteDatabase"),
      ]),
    .testTarget(
      name: "SQLiteDatabaseTests",
      dependencies: ["SQLiteDatabase", "FileLock"]),
  ]
)
