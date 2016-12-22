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


enum SQLiteError: Error {
    case Open(errorString: String)
    case Close(errorString: String)
    case Exec(errorString: String)
    case Prepare(errorString: String)
    case Step
    case DatabaseNotOpen
    case Unknown
}

enum SQLiteType {
    case Integer
    case Float
    case Text
    case Blob
    case Null
    case Unknown
}


struct Entity {
    let name: String
    
    init (name: String) {
        self.name = name
    }
}


class DatabaseContext {
    let databasePath: String?
    
    private var databaseConnection: DatabaseConnection?
    private let entities: Array<Entity> = Array()
    private let queue: DispatchQueue
    
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
