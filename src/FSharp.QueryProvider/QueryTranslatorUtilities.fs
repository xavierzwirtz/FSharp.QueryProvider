module FSharp.QueryProvider.QueryTranslatorUtilities
    
open FSharp.QueryProvider.DataReader
open FSharp.QueryProvider.PreparedQuery
open FSharp.QueryProvider.Expression
open FSharp.QueryProvider.ExpressionMatching
open System.Linq.Expressions

open Microsoft.FSharp.Reflection

type IQueryable = System.Linq.IQueryable
type IQueryable<'T> = System.Linq.IQueryable<'T>

module List =
    let interpolate (toInsert : 'T list) (list : 'T list) : 'T list=
        list 
        |> List.fold(fun acum item -> 
            match acum with 
            | [] -> [item]
            | _ -> acum @ toInsert @ [item]
        ) []

type Context = {
    TableAlias : string list option
    TopQuery : bool
}

type DBType<'t>= 
| Unhandled
| DataType of 't

type TypeSource = 
| Method of System.Reflection.MethodInfo
| Property of System.Reflection.PropertyInfo
| Value of obj
| Type of System.Type

type GetDBType<'t> = TypeSource -> DBType<'t>
type GetTableName = System.Type -> string option
type GetColumnName = System.Reflection.MemberInfo -> string option

let getLambda (m : MethodCallExpression) =
        (stripQuotes (m.Arguments.Item(1))) :?> LambdaExpression

let (|SingleSameSelect|_|) (l : LambdaExpression) =
    match l.Body with
    | :? ParameterExpression as paramAccess -> 
        let param = (l.Parameters |> Seq.exactlyOne)
        if l.Parameters.Count = 1 && paramAccess.Type = param.Type then
            Some param
        else 
            None
    | _ -> None

let invoke (m : MethodCallExpression) = 
    Expression.Lambda(m).Compile().DynamicInvoke()

let getMethod name (ml : MethodCallExpression list) = 
    let m = ml |> List.tryFind(fun m -> m.Method.Name = name)
    match m with
    | Some m -> 
        Some(m), (ml |> List.filter(fun ms -> ms <> m))
    | None -> None, ml

let getMethods names (ml : MethodCallExpression list) = 
    let methods = ml |> List.filter(fun m -> names |> List.exists(fun n -> m.Method.Name = n))
    methods, (ml |> List.filter(fun ms -> methods |> List.forall(fun m -> m <> ms)))

let splitResults source = 
    let fst = function
        | x, _, _ -> x
    let snd = function
        | _, x, _ -> x
    let third = function
        | _, _, x -> x

    source |> List.fold(fun a b ->
        fst(a) @ fst(b), 
        snd(a) @ snd(b), 
        third(a) @ third(b)
    ) (List.empty, List.empty, List.empty)

let unionExactlyOneCaseOneField t = 
    let cases = FSharpType.GetUnionCases t
    if cases |> Seq.length > 1 then
        failwith "Multi case unions are not supported"
    else
        let case = cases |> Seq.exactlyOne
        let fields = case.GetFields()
        if fields |> Seq.length > 1 then
            failwith "Only one field allowed on union case"
        else
            fields |> Seq.exactlyOne

let rec unwrapType (t : System.Type) = 
    if isOption t then
        t.GetGenericArguments() |> Seq.head |> unwrapType
    else if t |> FSharpType.IsUnion then
        (unionExactlyOneCaseOneField t).PropertyType |> unwrapType
    else
        t

let createParameter columnIndex value dbType = 
    let p = {
        PreparedParameter.Name = "@p" + columnIndex.ToString()
        Value = value
        DbType = dbType
    }

    [p.Name; ""], [p], List.empty<TypeConstructionInfo>

let rec unwrapValue value = 
    let t = value.GetType()
    if t |> isOption then
        t.GetMethod("get_Value").Invoke(value, [||]) |> unwrapValue
    else if t |> FSharpType.IsUnion then
        t.GetMethod("get_Item").Invoke(value, [||]) |> unwrapValue
    else
        value

let rec valueToQueryAndParam (columnIndex : int) (dbType : DBType<_>) (value : obj)= 
    let value = unwrapValue value
    match dbType with
    | Unhandled -> failwith "Shouldnt ever get to this point with this value"
    | DataType t ->
        createParameter columnIndex value t

let createNull (columnIndex : int) (dbType : DBType<_>) =
    match dbType with
    | Unhandled -> failwith "Shouldnt ever get to this point with this value" 
    | DataType dbType ->
        createParameter columnIndex System.DBNull.Value dbType

let getOperationsAndQueryable e : option<IQueryable * MethodCallExpression list> =
    let rec get (e : MethodCallExpression) : option<IQueryable option * MethodCallExpression list> = 
        match e with
        | CallIQueryable(e, q, _args) -> 
            match q with
            | Constant c -> Some(Some(c.Value :?> IQueryable), [e])
            | Call m -> 
                let r = get(m)
                match r with 
                | Some (q, m) -> 
                    Some (q, m |> List.append([e]))
                | None -> None
            | _ -> failwithf "not implemented nodetype '%A'" q.NodeType
        | _ ->
            if e.Arguments.Count = 0 then
                if typedefof<IQueryable>.IsAssignableFrom e.Type then
                    Some (Some (invoke(e) :?> IQueryable), [])
                else
                    None
            else
                None

    let result = get e
    match result with 
    | None -> None
    | Some result -> 
        Some(fst(result).Value, snd(result))

open System.Reflection
let getLocalValue (e : Expression) : obj option =

    let rec getChain (e : Expression) : option<obj * MemberExpression list> = 
        match e with 
        | MemberAccess m -> 
            match m.Expression with 
            | Constant c ->
                Some (c.Value, [m])
            | MemberAccess ma ->
                match getChain ma with 
                | Some (c, ml) -> 
                    Some (c, ml |> List.append([m]))
                | None -> None
            | Call call -> Some (invoke call, [m])
            | _ -> None
        | _ -> None

    match getChain e with
    | Some (root, accesses) -> 
        let result = 
            accesses
            |> List.rev
            |> List.fold(fun state item ->
                if item.Member.MemberType = MemberTypes.Property then
                    let prop = item.Member :?> PropertyInfo
                    prop.GetValue(state,[||])
                else if item.Member.MemberType = MemberTypes.Field then //ew field
                    let field = item.Member :?> FieldInfo
                    field.GetValue(state)
                else
                    failwithf "not implemented member type '%A'" item.Member.MemberType
            ) root
        Some result
    | None -> None