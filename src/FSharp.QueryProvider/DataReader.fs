module FSharp.QueryProvider.DataReader

open System.Collections


type ManyOrOne = 
| Many
| One

type TypeConstructionInfo = {
    ManyOrOne : ManyOrOne
    Type : System.Type
    ConstructorArgs : int seq
    PropertySets : (int * System.Reflection.PropertyInfo) seq
}
//
//let constructResult (reader : System.Data.IDataReader) (types : TypeConstructionInfo) : obj =
//    
//
//let read (reader : System.Data.IDataReader) (resultConstructionInfo : ResultConstructionInfo) : obj = 
//    let constructResult () = 
//        constructResult reader res.Types
//
//    let res = resultConstructionInfo
//    let constructed : obj = 
//        match res.ManyOrOne with
//        | Many -> 
//            null
//            //res.Types
//            
//        | One -> 
//            let readResult = reader.Read()
//            if readResult then
//                constructResult()
//            else
//                null
//
//    constructed