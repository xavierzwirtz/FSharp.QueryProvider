module FSharp.QueryProvider.DataReader

open System.Collections
open Microsoft.FSharp.Reflection

type ReturnType = 
| Single
| SingleOrDefault
| Many
// | Group of GroupExpression

type ConstructionInfo = {
    ReturnType : ReturnType
    Type : System.Type
    TypeOrLambda : TypeOrLambdaConstructionInfo
}

and TypeOrLambdaConstructionInfo = 
| Type of TypeConstructionInfo
| Lambda of LambdaConstructionInfo

and TypeOrValueConstructionInfo = 
| Type of TypeConstructionInfo
| Value of int

and TypeConstructionInfo = {
    Type : System.Type
    ConstructorArgs : TypeOrValueConstructionInfo seq
    PropertySets : (TypeOrValueConstructionInfo * System.Reflection.PropertyInfo) seq
}

and LambdaConstructionInfo = {
    Lambda : System.Linq.Expressions.LambdaExpression
    Parameters : TypeOrValueConstructionInfo seq
}

let createTypeConstructionInfo t constructorArgs propertySets =
    {
        Type = t
        ConstructorArgs = constructorArgs
        PropertySets = propertySets
    }

let isValueType (t : System.Type) =
    t.IsValueType || t = typedefof<string>
let isOption (t : System.Type) = 
    t.IsGenericType &&
    t.GetGenericTypeDefinition() = typedefof<Option<_>>

let rec constructResult (reader : System.Data.IDataReader) (ctor : ConstructionInfo) : obj =
    
    match ctor.TypeOrLambda with
    | TypeOrLambdaConstructionInfo.Lambda l ->  
        let paramValues = 
            l.Parameters 
            |> Seq.map(fun p -> 
                match p with
                | Type typeCtor -> constructType reader typeCtor
                | Value i -> reader.GetValue i
            )
        l.Lambda.Compile().DynamicInvoke(paramValues |> Seq.toArray)
    | TypeOrLambdaConstructionInfo.Type typeCtor ->
        constructType reader typeCtor

and constructType reader typeCtor = 
    let getSingleIndex() = 
        match typeCtor.ConstructorArgs |> Seq.exactlyOne with
        | Type _ -> failwith "Shouldnt be Type"
        | Value i -> i

    let getValue i = 
        let typeName = reader.GetDataTypeName i
        let value = reader.GetValue i 
        if typeName = "char" then
            let str = value :?> string 
            str.TrimEnd() :> obj
        else
            value

    let t = typeCtor.Type
    if t = typedefof<string> ||
        t = typedefof<bool> ||
        t = typedefof<byte> ||
        t = typedefof<sbyte> ||
        t = typedefof<char> ||
        t = typedefof<System.DateTime> ||
        t = typedefof<decimal> ||
        t = typedefof<double> ||
        t = typedefof<float> ||
        t = typedefof<System.Guid> ||
        t = typedefof<int16> ||
        t = typedefof<int32> ||
        t = typedefof<int64> then
        getValue (getSingleIndex())
    else if isOption t then
        let i = getSingleIndex()
        if reader.IsDBNull(i) then
            None :> obj
        else
            let value = reader.GetValue (i)
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
                | Value i -> getValue i
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
         
    let returnType = constructionInfo.ReturnType
    let t = constructionInfo.Type
    
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