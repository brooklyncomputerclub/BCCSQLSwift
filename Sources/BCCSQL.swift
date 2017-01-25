//
//  BCCSQL.swift
//
//
//  Created by Laurence Andersen on 12/20/16.
//
//

// TODO: Explicit column ordering
// TODO: Relationships/foreign keys support
// TODO: Sort descriptors
// TODO: NSPredicate clone?
// TODO: Transaction bundling/first class transaction objects
// TODO: Mass object import
// TODO: GCD support
// TODO: Prepared Statement caching
// TODO: BLOB Support
// TODO: In-memory object cache
// TODO: Observation?
// TODO: Versioning/handle DB incompatibility?
// TODO: Investigate memory management of OpaquePointers
// TODO: Handle property value conflicts/merging

#if os(Linux)
    import CSQLiteLinux
#else
    import CSQLiteMac
#endif

import Dispatch

let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

typealias SQLiteRawType = Int32
typealias KeyValuePair = (key: String, value: Any?)


enum SQLiteError: Error {
    case Open(errorString: String)
    case Close(errorString: String)
    case Exec(errorString: String)
    case Prepare(errorString: String)
    case Bind
    case Step
    case DatabaseNotOpen
    case Unknown
}


enum SQLiteType: String {
    case Integer = "INTEGER"
    case Float = "FLOAT"
    case Text = "TEXT"
    case Blob = "BLOB"
    case Null = "NULL"
    case Unknown
}


protocol ModelObject {
    static var entity: Entity { get }
    
    func setKey<T>(_ key: String, value: T)
}


class DatabaseContext {
    let databasePath: String?
    
    private var databaseConnection: DatabaseConnection?
    private let queue: DispatchQueue
    
    private var entities = [String: Entity]()
    
    init (databasePath dbPath: String) {
        databasePath = dbPath
        databaseConnection = nil
        
        queue = DispatchQueue(label: "nyc.bcc.SQLContext.WorkerQueue")
        //queue.setTarget(queue: DispatchQueue.global())
        
        initializeDatabase()
    }
    
    func initializeDatabase () {
        guard let dbPath = databasePath else {
            return
        }
        
        do {
            try databaseConnection = DatabaseConnection(forPath: dbPath)
        } catch SQLiteError.Open(let errorString) {
            print("Error opening database: \(errorString)")
        } catch {
            print("Unknown error")
        }
    }
    
    func initializeForModelObjectsOfType<U: ModelObject>(_ type: U.Type) {
        guard let db = databaseConnection else {
            return
        }
        
        let entity = U.entity
        
        do {
            guard let createSQL = entity.schemaSQL else {
                return
            }
            
            try db.exec(withSQLString: createSQL)
        } catch {
            print("Error creating entity table:")
        }
    }
    
    func createModelObjectOfType<U: ModelObject>(_ type:U.Type, withKeysAndValues keyValueList: KeyValuePair...) throws -> U? {
        return try createModelObjectOfType(type, withKeysAndValues: keyValueList)
    }
    
    func createModelObjectOfType<U: ModelObject>(_ type:U.Type, withKeysAndValues keyValueList: [KeyValuePair]) throws -> U? {
        guard let db = databaseConnection else {
            return nil
        }
        
        let keyList = keyValueList.map { (key, value) in
            return key
        }
        
        let valueList = keyValueList.map { key, value in
            return value
        }
        
        let entity = U.entity
        
        guard let insertSQL = entity.insertSQLForPropertyKeys(keyList) else {
            return nil
        }
        
        guard let insertStatement = try db.prepareStatement(withSQLString: insertSQL) else {
            return nil
        }
        
        try insertStatement.bind(values: valueList)
        try insertStatement.step()
        
        // TODO: Populate values in created object
        let modelObject = entity.create()
        
        try insertStatement.finalize()
        
        return modelObject as? U
    }
    
    func createOrUpdateModelObjectOfType<U: ModelObject>(_ type:U.Type, primaryKeyValue: Any, withKeysAndValues keyValueList: KeyValuePair...) throws -> U? {
        var modelObject: U? = nil
        
        if try modelObjectExistsForType(type, primaryKeyValue: primaryKeyValue) {
            modelObject = try updateModelObjectOfType(type, primaryKeyValue: primaryKeyValue, withKeysAndValues: keyValueList)
        } else {
            modelObject = try createModelObjectOfType(type, withKeysAndValues: keyValueList)
        }
        
        return modelObject
    }
    
    func updateModelObjectOfType<U: ModelObject>(_ type:U.Type, primaryKeyValue: Any, withKeysAndValues keyValueList: KeyValuePair...) throws -> U? {
        return try updateModelObjectOfType(type, primaryKeyValue: primaryKeyValue, withKeysAndValues: keyValueList)
    }
    
    func updateModelObjectOfType<U: ModelObject>(_ type:U.Type, primaryKeyValue: Any, withKeysAndValues keyValueList: [KeyValuePair]) throws -> U? {
        
        guard let db = databaseConnection else {
            return nil
        }
        
        let keyList = keyValueList.map { (key, value) in
            return key
        }
        
        let valueList = keyValueList.map { key, value in
            return value
        }
        
        let entity = U.entity
        
        guard let updateSQL = entity.updateSQLForKeys(keyList) else {
            return nil
        }
        
        guard let updateStatement = try db.prepareStatement(withSQLString: updateSQL) else {
            return nil
        }
        
        try updateStatement.bind(values: valueList)
        try updateStatement.bind(item: primaryKeyValue, atIndex: valueList.count + 2)
        try updateStatement.step()
        try updateStatement.finalize()
        
        // TODO: Populate values in created object
        let modelObject = entity.create()
        
        return modelObject as? U
    }
    
    func deleteModelObjectOfType<U: ModelObject>(_ type:U.Type, primaryKeyValue: Any) throws {
        guard let db = databaseConnection else {
            return
        }
        
        let entity = U.entity
        
        guard let deleteSQL = entity.deleteByPrimaryKeySQL else {
            return
        }
        
        guard let deleteStatement = try db.prepareStatement(withSQLString: deleteSQL) else {
            return
        }
        
        try deleteStatement.bind(item: primaryKeyValue, atIndex: 1)
        
        try deleteStatement.step()
    }
    
    func modelObjectExistsForType<U: ModelObject>(_ type:U.Type, primaryKeyValue: Any) throws -> Bool {
        guard let db = databaseConnection else {
            return false
        }
        
        let entity = U.entity
        
        guard let findSQL = entity.findByPrimaryKeySQL(includingRelationships: true) else {
            return false
        }
        
        guard let findStatement = try db.prepareStatement(withSQLString: findSQL) else {
            return false
        }
        
        try findStatement.bind(item: primaryKeyValue, atIndex: 1)
        
        var found = false
        
        if try findStatement.step() {
            found = true
        }
        
        try findStatement.finalize()
        
        return found
    }
    
    func findModelObjectOfType<U: ModelObject>(_ type:U.Type, primaryKeyValue: Any) throws -> U? {
        guard let db = databaseConnection else {
            return nil
        }
        
        let entity = U.entity
        
        guard let findSQL = entity.findByPrimaryKeySQL() else {
            return nil
        }
        
        guard let findStatement = try db.prepareStatement(withSQLString: findSQL) else {
            return nil
        }
        
        try findStatement.bind(item: primaryKeyValue, atIndex: 1)
        
        let foundObject = try nextModelObjectOfType(type, fromStatement: findStatement)
        
        try findStatement.finalize()
        
        return foundObject
    }
    
    func nextModelObjectOfType<U: ModelObject>(_ type:U.Type, fromStatement statement: DatabaseConnection.Statement) throws -> U? {
        guard try statement.step() else {
            return nil
        }
        
        let entity = U.entity
        guard let modelObject = entity.create() else {
            return nil
        }
        
        // TODO: How to get property for column (preferably by index, which means entity needs to keep order)
        
        let columnCount = Int(statement.columnCount)
        
        for index in 0...columnCount {
            let int32Index = Int32(index)
            
            guard let columnName = statement.columnName(forIndex: int32Index) else {
                continue
            }
            
            guard let property = entity.propertyForColumnName(columnName) else {
                continue
            }
            
            let columnType = statement.columnType(forIndex: int32Index)
            
            switch columnType {
            case .Integer:
                let intValue = statement.readInteger(atColumnIndex: int32Index)
                modelObject.setKey(property.key, value: intValue)
            case .Float:
                let floatValue = statement.readFloat(atColumnIndex: int32Index)
                modelObject.setKey(property.key, value: floatValue)
            case .Text:
                let textValue = statement.readText(atColumnIndex: int32Index)
                modelObject.setKey(property.key, value: textValue)
            case .Blob:
                let blobValue = statement.readBlob(atColumnIndex: int32Index)
                modelObject.setKey(property.key, value: blobValue)
            default:
                continue
            }
        }
        
        return modelObject as? U
    }
}


class Entity {
    let name: String
    let tableName: String
    
    private var properties = [String: Property]()
    var primaryKeyPropertyKey: String? = nil
    
    private var relationships: [Relationship] = Array<Relationship>()
    
    let create: () -> ModelObject?
    
    var schemaSQL: String? {
        get {
            var sqlString = "CREATE TABLE IF NOT EXISTS \(tableName)"
            guard properties.count > 0 else {
                return sqlString
            }
            
            sqlString.append(" (")
            
            for (index, currentItem) in properties.enumerated() {
                let currentProperty = currentItem.value
                
                guard let columnSQL = currentProperty.schemaSQL else {
                    continue
                }
                
                sqlString.append(columnSQL)
                
                if currentProperty.key == self.primaryKeyPropertyKey {
                    sqlString.append(" PRIMARY KEY")
                }
                
                if index < (properties.count - 1) {
                    sqlString.append(", ")
                }
            }
            
            // TODO: Create columns for relationships too
            
            sqlString.append(")")
            
            return sqlString
        }
    }
    
    var selectSQL: String? {
        return "SELECT \(columnsListString) FROM \(tableName)"
    }
    
    var findByRowIDSQL: String? {
        return "SELECT \(columnsListString) FROM \(tableName) WHERE rowid = ?"
    }
    
    var deleteSQL: String? {
        return "DELETE FROM \(tableName)"
    }
    
    var deleteByPrimaryKeySQL: String? {
        get {
            guard let pkp = primaryKeyProperty else {
                return nil
            }
            
            return "DELETE FROM \(tableName) WHERE \(pkp.columnName) = ?"
        }
    }
    
    var primaryKeyProperty: Property? {
        guard let primaryKey = self.primaryKeyPropertyKey else {
            return nil
        }
        
        return self[primaryKey]
    }
    
    var columnsListString: String {
        guard properties.count > 0 else {
            return "*"
        }
        
        var columnsString = String()
        
        for (index, currentItem) in properties.enumerated() {
            let currentProperty = currentItem.value
            
            if index > 0 {
                columnsString.append(", ")
            }
            
            columnsString.append(currentProperty.columnName)
        }
        
        return columnsString
    }
    
    subscript(key: String) -> Property? {
        get {
            return propertyForKey(key)
        }
    }
    
    required init<ModelObjectType: ModelObject> (name: String, tableName: String, modelInstanceCreator: @escaping () -> ModelObjectType?) {
        self.name = name
        self.tableName = tableName
        self.create = modelInstanceCreator
    }
    
    func addProperty(_ property: Property, isPrimaryKey: Bool = false) {
        properties[property.key] = property
        
        if isPrimaryKey {
            self.primaryKeyPropertyKey = property.key
        }
    }
    
    func propertyForKey(_ key: String) -> Property? {
        return properties[key]
    }
    
    func propertyForColumnName(_ columnName: String) -> Property? {
        return properties.filter ({ (columnName, currentProperty) -> Bool in
            return (currentProperty.columnName == columnName)
        }).first?.value
    }
    
    func addRelationshipToModelObjectOfType<U: ModelObject>(_ type: U.Type, propertyKey: String, foreignPropertyKey: String? = nil) {
        relationships.append(Relationship(propertyKey: propertyKey, foreignEntityType: U.self, foreignPropertyKey: foreignPropertyKey))
    }
    
    func insertSQLForPropertyKeys(_ keys: Array<String>) -> String? {
        guard properties.count > 0 else {
            return nil
        }
        
        var columnsString = String()
        var valuesString = String()
        
        for (index, currentKey) in keys.enumerated() {
            guard let currentProperty = self[currentKey] else {
                continue
            }
            
            if (index > 0) {
                columnsString.append(", ")
                valuesString.append(", ")
            }
            
            columnsString.append(currentProperty.columnName)
            valuesString.append("?")
        }
        
        return "INSERT INTO \(tableName) (\(columnsString)) VALUES (\(valuesString))"
    }
    
    func updateSQLForKeys(_ keys: Array<String>) -> String? {
        guard properties.count > 0, let primaryKeyColumn = primaryKeyProperty?.columnName else {
            return nil
        }
        
        var assignmentsString = String()
        
        for (index, currentKey) in keys.enumerated() {
            guard let columnName = self[currentKey]?.columnName else {
                continue
            }
            
            assignmentsString.append("\(columnName) = ?")
            
            if index > 0 {
                assignmentsString.append(", ")
            }
        }
        
        return "UPDATE \(tableName) SET \(assignmentsString) WHERE \(primaryKeyColumn) = ?"
    }
    
    func findByPrimaryKeySQL(includingRelationships: Bool = true) -> String? {
        guard let pkp = primaryKeyProperty else {
            return nil
        }
        
        let primaryKeyColumn = pkp.columnName
        
        var findSQL = "SELECT \(columnsListString) FROM \(tableName) WHERE \(primaryKeyColumn) = ?"
        
        if !includingRelationships && relationships.count > 0 {
            return findSQL
        }
        
        for currentRelationship in relationships {
            let foreignEntity = currentRelationship.foreignEntityType.entity
            let foreignTable = foreignEntity.tableName
            
            var foreignProperty: Property?
            if let foreignPropertyKey = currentRelationship.foreignPropertyKey {
                foreignProperty = foreignEntity.propertyForKey(foreignPropertyKey)
            } else {
                foreignProperty = foreignEntity.primaryKeyProperty
            }
            
            guard let foreignColumnName = foreignProperty?.columnName else {
                continue
            }
            
            let joinSQL = " LEFT JOIN \(foreignTable) ON \(tableName).\(primaryKeyColumn) = \(foreignTable).\(foreignColumnName)"
            
            findSQL.append(joinSQL)
        }
        
        return findSQL
    }
}


class Property {
    let key: String
    let columnName: String
    let sqlType: SQLiteType
    var nonNull: Bool = false
    var unique: Bool = false
    
    var schemaSQL: String? {
        get {
            var sqlString = columnName
            
            guard sqlType != .Unknown else {
                return nil
            }
            
            sqlString.append(" \(sqlType.rawValue)")
            
            if nonNull == true {
                sqlString.append(" NOT NULL")
            }
            
            if unique == true {
                sqlString.append(" UNIQUE")
            }
            
            return sqlString
        }
    }
    
    init (withKey key: String, columnName: String, type: SQLiteType) {
        self.key = key
        self.columnName = columnName
        self.sqlType = type
    }
}


class DatabaseConnection {
    private var database: OpaquePointer? = nil
    
    var lastInsertRowID: Int? {
        guard let db = database else {
            return nil
        }
        
        let rowID = sqlite3_last_insert_rowid(db)
        return Int(rowID)
    }
    
    init(forPath dbPath: String) throws {
        let options = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        
        let err = sqlite3_open_v2(dbPath, &database, options, nil)
        if err == SQLITE_OK {
            return
        }
        
        if let errorString = errorString(forCode: err) {
            throw SQLiteError.Open(errorString: errorString)
        } else {
            throw SQLiteError.Unknown
        }
    }
    
    func close () throws {
        guard database != nil else {
            throw SQLiteError.DatabaseNotOpen
        }
        
        let err = sqlite3_close(database)
        if err == SQLITE_OK {
            database = nil
            return
        }
        
        if let errorString = errorString(forCode: err) {
            throw SQLiteError.Close(errorString: errorString)
        } else {
            throw SQLiteError.Unknown
        }
    }
    
    func exec (withSQLString sqlString: String) throws {
        guard database != nil else {
            throw SQLiteError.DatabaseNotOpen
        }
        
        var errString: UnsafeMutablePointer<CChar>?
        let err = sqlite3_exec(database, sqlString, nil, nil, &errString)
        if err == SQLITE_OK {
            return
        }
        
        if errString != nil {
            throw SQLiteError.Exec(errorString: String(cString: errString!))
        } else {
            throw SQLiteError.Unknown
        }
    }
    
    func prepareStatement(withSQLString sqlString: String) throws -> Statement? {
        guard database != nil else {
            throw SQLiteError.DatabaseNotOpen
        }
        
        var statement: OpaquePointer?
        let err = sqlite3_prepare_v2(database, sqlString, -1, &statement, nil);
        if err == SQLITE_OK {
            return Statement(withSQLString: sqlString, statement: statement!)
        }
        
        if let errorString = errorString(forCode: err) {
            throw SQLiteError.Prepare(errorString: errorString)
        } else {
            throw SQLiteError.Unknown
        }
    }
    
    class Blob {
        
        init(pointer: UnsafeRawPointer, length: Int) {
            
        }
        
        deinit {
            
        }
        
    }
    
    struct Statement {
        var name: String?
        let SQLString: String
        let statement: OpaquePointer
        
        var columnCount: Int32 {
            return sqlite3_column_count(statement)
        }
        
        init (withSQLString sqlString: String, statement: OpaquePointer) {
            self.SQLString = sqlString
            self.statement = statement
        }
        
        func columnName(forIndex index: Int32) -> String? {
            if let columnName = sqlite3_column_name(statement, index) {
                return String(cString: columnName)
            }
            
            return nil
        }
        
        func columnType(forIndex index: Int32) -> SQLiteType {
            let sqliteRawType = sqlite3_column_type(statement, index)
            return columnTypeForSQLiteRawType(sqliteRawType)
        }
        
        func readInteger(atColumnIndex columnIndex: Int32) -> Int {
            let intValue = sqlite3_column_int(statement, columnIndex)
            return Int(intValue)
        }
        
        func readFloat(atColumnIndex columnIndex: Int32) -> Double {
            let doubleValue = sqlite3_column_double(statement, columnIndex)
            return Double(doubleValue)
        }
        
        func readText(atColumnIndex columnIndex: Int32) -> String? {
            guard let textValue = sqlite3_column_text(statement, columnIndex) else {
                return nil
            }
            
            return String.decodeCString(textValue, as: UTF8.self)?.result
        }
        
        func readBlob(atColumnIndex columnIndex: Int32) -> Blob? {
            guard let blobValue = sqlite3_column_blob(statement, columnIndex) else {
                return nil
            }
            
            let byteLength = sqlite3_column_bytes(statement, columnIndex)
            
            return Blob(pointer: blobValue, length: Int(byteLength))
        }
        
        func bind(values: Array<Any?>) throws {
            for (index, currentValue) in values.enumerated() {
                try bind(item: currentValue, atIndex: Int32(index) + 1)
            }
        }
        
        func bind(item:Any?, atIndex index: Int32) throws {
            guard let value = item else {
                sqlite3_bind_null(statement, index)
                return
            }
            
            if let intItem = value as? Int {
                sqlite3_bind_int(statement, index, Int32(intItem))
            } else if let integerItem = value as? Int32 {
                sqlite3_bind_int(statement, index, integerItem)
            } else if let longItem = value as? Int64 {
                sqlite3_bind_int64(statement, index, longItem)
            } else if let floatItem = value as? Double {
                sqlite3_bind_double(statement, index, floatItem)
            } else if let stringItem = value as? String {
                sqlite3_bind_text(statement, index, stringItem, -1, SQLITE_TRANSIENT)
            } else if let blobItem = value as? Array<UInt8> {
                sqlite3_bind_blob(statement, index, blobItem, Int32(blobItem.count), SQLITE_TRANSIENT)
            } else {
                throw SQLiteError.Bind
            }
        }
        
        @discardableResult func step () throws -> Bool {
            let err = sqlite3_step(statement)
            if err == SQLITE_ROW {
                return true
            } else if err == SQLITE_DONE {
                return false
            }
            
            // TODO: Should only throw an exception here in case of a definite error
            throw SQLiteError.Step
        }
        
        func finalize () throws {
            sqlite3_finalize(statement)
        }
    }
}

struct Relationship {
    let propertyKey: String
    let foreignEntityType: ModelObject.Type
    let foreignPropertyKey: String?
}


func columnTypeForSQLiteRawType(_ sqliteType: SQLiteRawType) -> SQLiteType {
    switch sqliteType {
    case SQLITE_INTEGER:
        return .Integer
    case SQLITE_FLOAT:
        return .Float
    case SQLITE_TEXT:
        return .Text
    case SQLITE_BLOB:
        return .Blob
    case SQLITE_NULL:
        return .Null
    default:
        return .Unknown
    }
}

func errorString(forCode err: Int32) -> String? {
    if let errString: UnsafePointer<CChar> = sqlite3_errstr(err) {
        return String.init(cString: errString)
    }
    
    return nil
}

