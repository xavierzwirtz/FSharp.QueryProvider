module LocalDataReader

open System.Data
open System.Data.Common

type LocalDataReader(data : obj list list) =
    let mutable isOpen = true
    let mutable currentIndex = -1
    let current() = 
        if currentIndex = -1 then
            failwith "cannot access until Read has been called."
        data |> List.nth <| currentIndex
    let field index =
        current() |> List.nth <| index
    interface IDataReader with
        member x.Close(): unit = 
            isOpen <- false
        
        member x.Depth: int = 
            failwith "Not implemented yet"
        
        member x.Dispose(): unit = 
            (x :> IDataReader).Close()
        
        member x.FieldCount: int = 
            current() |> List.length

        member x.GetBoolean(i: int): bool = 
            field i :?> bool
        
        member x.GetByte(i: int): byte = 
            field i :?> byte
        
        member x.GetBytes(_i: int, _fieldOffset: int64, _buffer: byte [], _bufferoffset: int, _length: int): int64 = 
            failwith "Not implemented yet"
        
        member x.GetChar(i: int): char = 
            field i :?> char
        
        member x.GetChars(_i: int, _fieldoffset: int64, _buffer: char [], _bufferoffset: int, _length: int): int64 = 
            failwith "Not implemented yet"
        
        member x.GetData(_i: int): IDataReader = 
            failwith "Not implemented yet"
        
        member x.GetDataTypeName(_i: int): string = 
            ""

        member x.GetDateTime(i: int): System.DateTime = 
            field i :?> System.DateTime
        
        member x.GetDecimal(i: int): decimal = 
            field i :?> decimal
        
        member x.GetDouble(i: int): float = 
            field i :?> float
        
        member x.GetFieldType(i: int): System.Type = 
            (field i).GetType()
        
        member x.GetFloat(i: int): float32 = 
            field i :?> float32
        
        member x.GetGuid(i: int): System.Guid = 
            field i :?> System.Guid
        
        member x.GetInt16(i: int): int16 = 
            field i :?> int16
        
        member x.GetInt32(i: int): int = 
            field i :?> int
        
        member x.GetInt64(i: int): int64 = 
            field i :?> int64
        
        member x.GetName(_i: int): string = 
            failwith "Not implemented yet"
        
        member x.GetOrdinal(_name: string): int = 
            failwith "Not implemented yet"
        
        member x.GetSchemaTable(): DataTable = 
            failwith "Not implemented yet"
        
        member x.GetString(i: int): string = 
            field i :?> string
        
        member x.GetValue(i: int): obj = 
            field i
        
        member x.GetValues(_values: obj []): int = 
            failwith "Not implemented yet"
        
        member x.IsClosed: bool = 
            not isOpen 
        
        member x.IsDBNull(i: int): bool = 
            match (field i) with
            | :? System.DBNull as v -> v = System.DBNull.Value
            | _ -> false
        
        member x.Item
            with get (i: int): obj = 
                (x :> IDataReader).GetValue i
        
        member x.Item
            with get (_name: string): obj = 
                failwith "Not implemented yet"
        
        member x.NextResult(): bool = 
            failwith "Not implemented yet"
        
        member x.Read(): bool = 
            currentIndex <- currentIndex + 1
            if currentIndex >= (data |> List.length) then
                false
            else
                true
        
        member x.RecordsAffected: int = 
            data |> List.length       