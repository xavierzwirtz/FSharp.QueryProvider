module FSharp.QueryProvider.DataReader

open System.Collections
open Microsoft.FSharp.Reflection

type ReturnType = 
| Single
| SingleOrDefault
| Many

type TypeOrLambdaConstructionInfo = 
| Type of TypeConstructionInfo
| Lambda of LambdaConstructionInfo

and TypeOrValueOrLambdaConstructionInfo = 
| Type of TypeConstructionInfo
| Value of int
| Bool of int
| Lambda of LambdaConstructionInfo

and TypeConstructionInfo = {
    Type : System.Type
    ConstructorArgs : TypeOrValueOrLambdaConstructionInfo seq
    PropertySets : (TypeOrValueOrLambdaConstructionInfo * System.Reflection.PropertyInfo) seq
}

and LambdaConstructionInfo = {
    Lambda : System.Linq.Expressions.LambdaExpression
    Parameters : TypeOrValueOrLambdaConstructionInfo seq
}

type ConstructionInfo = {
    ReturnType : ReturnType
    Type : System.Type
    TypeOrLambda : TypeOrLambdaConstructionInfo
    PostProcess : System.Linq.Expressions.LambdaExpression option
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

let sqlBoolToBool value =
    match value with
    | 0 -> false
    | 1 -> true
    | i -> failwith "%i" i

let rec constructResult (reader : System.Data.IDataReader) (ctor : ConstructionInfo) : obj =
    
    match ctor.TypeOrLambda with
    | TypeOrLambdaConstructionInfo.Lambda lambdaCtor ->  
        invokeLambda reader lambdaCtor
    | TypeOrLambdaConstructionInfo.Type typeCtor ->
        constructType reader typeCtor

and invokeLambda reader lambdaCtor = 
    let paramValues = 
        lambdaCtor.Parameters 
        |> Seq.map(fun p -> 
            match p with
            | TypeOrValueOrLambdaConstructionInfo.Type typeCtor -> constructType reader typeCtor
            | TypeOrValueOrLambdaConstructionInfo.Lambda lambdaCtor -> invokeLambda reader lambdaCtor
            | TypeOrValueOrLambdaConstructionInfo.Value i -> reader.GetValue i
            | TypeOrValueOrLambdaConstructionInfo.Bool i -> sqlBoolToBool ((reader.GetValue i) :?> int) :> obj
        )
    lambdaCtor.Lambda.Compile().DynamicInvoke(paramValues |> Seq.toArray)

and constructType reader typeCtor = 
    let getSingleIndex() = 
        match typeCtor.ConstructorArgs |> Seq.exactlyOne with
        | Type _ -> failwith "Shouldnt be Type"
        | Lambda _ -> failwith "Shouldnt be Lambda"
        | Bool i | Value i -> i

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
    else if t = typedefof<bool> then
        getValue (getSingleIndex()) :?> int |> sqlBoolToBool :> obj
    else if t.IsEnum then
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
                | Lambda l -> invokeLambda reader l
                | Bool i -> getValue (i) :?> int |> sqlBoolToBool :> obj
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

//let constructResults (reader : System.Data.IDataReader) (constructionInfo : ConstructionInfo) = 
//    Seq.empty<obj>
//    match constructionInfo.TypeOrLambda with
//    | Lambda lCtor -> lCtor.
//    
////    match returnType with
////    | Many -> 
////        let listT = typedefof<System.Collections.Generic.List<_>>
////        let conListT = listT.MakeGenericType([| t |])
////        let addM = conListT.GetMethods() |> Seq.find(fun m -> m.Name = "Add")
////        let inst = System.Activator.CreateInstance(conListT)
////        while reader.Read() do
////            let res = constructResult()
////            addM.Invoke(inst, [|res|]) |> ignore
////        inst
////    | Single | SingleOrDefault ->
////        if reader.Read() then
////            let r = constructResult()
////            if reader.Read() then
////                raise (System.InvalidOperationException "Sequence contains more than one element")
////            r
////        else
////            match returnType with
////            | Single -> raise (System.InvalidOperationException "Sequence contains no elements")
////            | SingleOrDefault -> 
////                if isValueType t then
////                    System.Activator.CreateInstance(t)
////                else
////                    null
////            | _ -> failwith "shouldnt be here"
let private moreThanOneMessage = "Sequence contains more than one element"
let private noElementsMessage = "Sequence contains no elements"

let read (reader : System.Data.IDataReader) constructionInfo : obj = 
    let constructResult () = 
        constructResult reader constructionInfo
         
    let returnType = constructionInfo.ReturnType
    let t = constructionInfo.Type
    let getAll() = 
        let listT = typedefof<System.Collections.Generic.List<_>>
        let conListT = listT.MakeGenericType([| t |])
        let addM = conListT.GetMethods() |> Seq.find(fun m -> m.Name = "Add")
        let inst = System.Activator.CreateInstance(conListT)
        while reader.Read() do
            let res = constructResult()
            addM.Invoke(inst, [|res|]) |> ignore
        inst
    match returnType with
    | Many -> 
        let inst = getAll()
        match constructionInfo.PostProcess with
        | Some postProcess -> postProcess.Compile().DynamicInvoke([|inst|])
        | None -> inst
    | Single | SingleOrDefault ->
        match constructionInfo.PostProcess with
        | Some postProcess -> postProcess.Compile().DynamicInvoke([|getAll()|])
        | None ->
            if reader.Read() then
                let r = constructResult()
                if reader.Read() then
                    raise (System.InvalidOperationException moreThanOneMessage)
                r
            else
                match returnType with
                | Single -> raise (System.InvalidOperationException noElementsMessage)
                | SingleOrDefault -> 
                    if isValueType t then
                        System.Activator.CreateInstance(t)
                    else
                        null
                | _ -> failwith "shouldnt be here"
