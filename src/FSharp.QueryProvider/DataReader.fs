module FSharp.QueryProvider.DataReader

open System.Collections
open Microsoft.FSharp.Reflection

type ReturnType = 
| Single
| SingleOrDefault
| Many
// | Group of GroupExpression

type ConstructionInfo = 
| Type of TypeConstructionInfo
| Lambda of LambdaConstructionInfo

and TypeOrValueConstructionInfo = 
| Type of TypeConstructionInfo
| Value of int

and TypeConstructionInfo = {
    Type : System.Type
    ReturnType : ReturnType
    ConstructorArgs : TypeOrValueConstructionInfo seq
    PropertySets : (TypeOrValueConstructionInfo * System.Reflection.PropertyInfo) seq
}

and LambdaConstructionInfo = {
    Type : System.Type
    ReturnType : ReturnType
    Lambda : System.Linq.Expressions.LambdaExpression
    Parameters : TypeOrValueConstructionInfo seq
}

let createTypeConstructionInfo t returnType constructorArgs propertySets =
    {
        Type = t
        ReturnType = returnType
        ConstructorArgs = constructorArgs
        PropertySets = propertySets
    }

let isValueType (t : System.Type) =
    t.IsValueType || t = typedefof<string>
let isOption (t : System.Type) = 
    t.IsGenericType &&
    t.GetGenericTypeDefinition() = typedefof<Option<_>>

let rec constructResult (reader : System.Data.IDataReader) (ctor : ConstructionInfo) : obj =
    
    match ctor with
    | ConstructionInfo.Lambda l ->  
        let paramValues = 
            l.Parameters 
            |> Seq.map(fun p -> 
                match p with
                | Type typeCtor -> constructType reader typeCtor
                | Value i -> reader.GetValue i
            )
        l.Lambda.Compile().DynamicInvoke(paramValues |> Seq.toArray)
    | ConstructionInfo.Type typeCtor ->
        constructType reader typeCtor

and constructType reader typeCtor = 
    let getSingleIndex() = 
        match typeCtor.ConstructorArgs |> Seq.exactlyOne with
        | Type _ -> failwith "Shouldnt be Type"
        | Value i -> i

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
    else if isOption t then
        let value = reader.GetValue (getSingleIndex())
        if value <> null then
            t.GetMethod("Some").Invoke(null, [| value |])
        else
            None :> obj
    else
        let ctorArgs = 
            typeCtor.ConstructorArgs
            |> Seq.map(fun arg -> 
                match arg with 
                | Type t -> constructType reader t
                | Value i -> reader.GetValue i
            )

        let inst = 
            if FSharpType.IsRecord t then
                FSharpValue.MakeRecord(t, (ctorArgs |> Seq.toArray))
            else
                System.Activator.CreateInstance(t, ctorArgs |> Seq.toArray)

        if typeCtor.PropertySets |> Seq.length > 0 then
            failwith "propertySets are not implemented"
        
        inst

let read (reader : System.Data.IDataReader) constructionInfo : obj = 
    let constructResult () = 
        constructResult reader constructionInfo
         
    let returnType, t = 
        match constructionInfo with
        | ConstructionInfo.Lambda l -> l.ReturnType, l.Type
        | ConstructionInfo.Type t -> t.ReturnType, t.Type

    match returnType with
    | Many -> 
        let listT = typedefof<System.Collections.Generic.List<_>>
        let conListT = listT.MakeGenericType([| t |])
        let addM = conListT.GetMethods() |> Seq.find(fun m -> m.Name = "Add")
        let inst = System.Activator.CreateInstance(conListT)
        while reader.Read() do
            let res = constructResult()
            addM.Invoke(inst, [|res|]) |> ignore
        inst
    | Single | SingleOrDefault ->
        if reader.Read() then
            let r = constructResult()
            if reader.Read() then
                raise (System.InvalidOperationException "Sequence contains more than one element")
            r
        else
            match returnType with
            | Single -> raise (System.InvalidOperationException "Sequence contains no elements")
            | SingleOrDefault -> 
                if isValueType t then
                    System.Activator.CreateInstance(t)
                else
                    null
            | _ -> failwith "shouldnt be here"