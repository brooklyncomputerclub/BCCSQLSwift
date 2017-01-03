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


class Entity {
    let name: String
    let tableName: String
    
    private var properties = [String: Property]()
    var primaryKeyPropertyKey: String? = nil
    
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
    
    init (name: String, tableName: String) {
        self.name = name
        self.tableName = tableName
    }
    
    func addProperty(_ property: Property, isPrimaryKey: Bool) {
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


class DatabaseContext {
    let databasePath: String?
    
    private var databaseConnection: DatabaseConnection?
    private let queue: DispatchQueue
    
    private var entities = [String: Entity]()
    
    init (databasePath dbPath: String) {
        databasePath = dbPath
        databaseConnection = nil
        
        queue = DispatchQueue(label: "nyc.bcc.SQLContext.WorkerQueue")
        queue.setTarget(queue: DispatchQueue.global())
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
            let sqliteType = sqlite3_column_type(statement, index)
            
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
        
        func bind(item:AnyObject, atIndex index: Int32) throws {
            let sqlType = columnType(forIndex: index)
            
            switch sqlType {
            case .Integer:
                if let integerItem = item as? Int64 {
                    sqlite3_bind_int64(statement, index, integerItem)
                } else {
                    throw SQLiteError.Bind
                }
            case .Float:
                if let floatItem = item as? Double {
                    sqlite3_bind_double(statement, index, floatItem)
                }
            case .Text:
                if let stringItem = item as? String {
                    sqlite3_bind_text(statement, index, stringItem, -1, SQLITE_TRANSIENT)
                } else {
                    throw SQLiteError.Bind
                }
            case .Blob:
                if let blobItem = item as? Array<UInt8> {
                    sqlite3_bind_blob(statement, index, blobItem, Int32(blobItem.count), SQLITE_TRANSIENT)
                } else {
                    throw SQLiteError.Bind
                }
            default:
                throw SQLiteError.Bind
            }
            
        }
        
        func step () throws {
            let err = sqlite3_step(statement)
            if err == SQLITE_ROW {
                return
            }
            
            throw SQLiteError.Step
        }
        
        func finalize () throws {
            sqlite3_finalize(statement)
        }
    }
}


func errorString(forCode err: Int32) -> String? {
    if let errString: UnsafePointer<CChar> = sqlite3_errstr(err) {
        return String.init(cString: errString)
    }
    
    return nil
}

