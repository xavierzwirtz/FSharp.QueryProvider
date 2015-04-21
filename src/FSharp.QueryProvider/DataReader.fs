module FSharp.QueryProvider.DataReader

open System.Collections
open Microsoft.FSharp.Reflection

type ManyOrOne = 
| Many
| One

type TypeConstructionInfo = {
    ManyOrOne : ManyOrOne
    Type : System.Type
    ConstructorArgs : int seq
    PropertySets : (int * System.Reflection.PropertyInfo) seq
}

let constructResult (reader : System.Data.IDataReader) (typeCtor : TypeConstructionInfo) : obj =
    
    let getSingleIndex() = typeCtor.ConstructorArgs |> Seq.exactlyOne
    let t = typeCtor.Type
    if t = typedefof<string> then
        reader.GetString (getSingleIndex()) :> obj
    else if t = typedefof<bool> then
        reader.GetBoolean (getSingleIndex()) :> obj
    else if t = typedefof<byte> then
        reader.GetByte (getSingleIndex()) :> obj
    //else if t = typedefof<sbyte> then
    else if t = typedefof<char> then
        reader.GetChar (getSingleIndex()) :> obj
    else if t = typedefof<System.DateTime> then
        reader.GetDateTime (getSingleIndex()) :> obj
    else if t = typedefof<decimal> then
        reader.GetDecimal (getSingleIndex()) :> obj
    else if t = typedefof<double> then
        reader.GetDouble (getSingleIndex()) :> obj
    else if t = typedefof<float> then
        reader.GetFloat (getSingleIndex()) :> obj
    else if t = typedefof<System.Guid> then
        reader.GetGuid (getSingleIndex()) :> obj
    else if t = typedefof<int16> then
        reader.GetInt16 (getSingleIndex()) :> obj
    else if t = typedefof<int32> then
        reader.GetInt32 (getSingleIndex()) :> obj
    else if t = typedefof<int64> then
        reader.GetInt64 (getSingleIndex()) :> obj
    else
        let ctorArgs = 
            typeCtor.ConstructorArgs
            |> Seq.map(fun sqlIndex -> 
                reader.GetValue sqlIndex
            )

        let inst = 
            if FSharpType.IsRecord t then
                FSharpValue.MakeRecord(t, (ctorArgs |> Seq.toArray))
            else
                System.Activator.CreateInstance(t, ctorArgs |> Seq.toArray)

        if typeCtor.PropertySets |> Seq.length > 0 then
            failwith "propertySets are not implemented"
        
        inst

let read (reader : System.Data.IDataReader) (typeCtor : TypeConstructionInfo) : obj = 
    let constructResult () = 
        constructResult reader typeCtor
         
    match typeCtor.ManyOrOne with
    | Many -> 
        let listT = typedefof<System.Collections.Generic.List<_>>
        let conListT = listT.MakeGenericType([| typeCtor.Type |])
        let addM = conListT.GetMethods() |> Seq.find(fun m -> m.Name = "Add")
        let inst = System.Activator.CreateInstance(conListT)
        while reader.Read() do
            let res = constructResult()
            addM.Invoke(inst, [|res|]) |> ignore
        inst
    | One -> 
        let readResult = reader.Read()
        if readResult then
            constructResult()
        else
            null