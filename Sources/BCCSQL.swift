//
//  BCCSQL.swift
//
//
//  Created by Laurence Andersen on 12/20/16.
//
//

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


class TestModelObject: ModelObject {
    var identifier: Int?
    var name: String?
    var city: String?
    
    static let entity: Entity = {
        let entity = Entity(name: "TestObject", tableName: "test_object", modelInstanceCreator: TestModelObject.init)
        
        entity.addProperty(Property(withKey: "identifier", columnName: "id", type: .Integer))
        entity.addProperty(Property(withKey: "name", columnName: "name", type: .Text))
        entity.addProperty(Property(withKey: "city", columnName: "city", type: .Text))
        
        return entity
    }()
    
    func setKey<T>(_ key: String, value: T) {
        switch key {
        case "identifier":
            self.identifier = value as? Int
        case "name":
            self.name = value as? String
        case "city":
            self.city = value as? String
        default:
            return
        }
    }
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
        
        createEntityTables()
    }
    
    func createEntityTables () {
        guard let dbConnection = databaseConnection else {
            return
        }
        
        for (_, currentEntity) in entities {
            do {
                guard let createSQL = currentEntity.schemaSQL else {
                    return
                }
                
                try dbConnection.exec(withSQLString: createSQL)
            } catch {
                print("Error creating entity tables:")
            }
        }
    }
    
    func addEntity(_ entity: Entity) {
        entities[entity.name] = entity
    }
    
    func entityForName(_ name: String) -> Entity? {
        return entities[name]
    }
    
    func createModelObjectOfType<U: ModelObject>(_ type:U.Type, withKeysAndValues keyValueList: KeyValuePair...) throws -> U? {
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
        
        if let insertStatement = try db.prepareStatement(withSQLString: insertSQL) {
            try insertStatement.bind(values: valueList)
            try insertStatement.step()
            try insertStatement.finalize()
        }
        
        let modelObject = entity.create()
        
        // Find object from database and return that
        
        return modelObject as? U
    }
    
    func nextModelObject<U: ModelObject>(fromStatement statement: DatabaseConnection.Statement) throws -> U? {
        try statement.step()
        
        let entity = U.entity
        guard let modelObject = entity.create() else {
            return nil
        }
        
        // How to get property for column (preferably by index, which means entity needs to keep order)
        // How to coerce raw DB value to instance value
        
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
            
            sqlString.append(")")
            
            return sqlString
        }
    }
    
    var selectSQL: String? {
        return "SELECT \(columnsListString) FROM \(tableName)"
    }
    
    var findByPrimaryKeySQL: String? {
        guard let pkp = primaryKeyProperty else {
            return nil
        }
        
        return "SELECT \(columnsListString) FROM \(tableName) WHERE = \(pkp.columnName)"
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
            
            columnsString.append(currentProperty.columnName)
            
            if index > 0 {
                columnsString.append(", ")
            }
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
    
    func updateSQLForPropertyKeys(_ keys: Array<String>, primaryKeyValue: AnyObject) -> String? {
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
}


struct Property {
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
            
            if let integerItem = value as? Int64 {
                sqlite3_bind_int64(statement, index, integerItem)
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
        
        func step () throws {
            let err = sqlite3_step(statement)
            if err == SQLITE_ROW || err == SQLITE_DONE {
                return
            }
            
            throw SQLiteError.Step
        }
        
        func finalize () throws {
            sqlite3_finalize(statement)
        }
    }
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

