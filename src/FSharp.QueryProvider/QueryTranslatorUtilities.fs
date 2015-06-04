module FSharp.QueryProvider.QueryTranslatorUtilities
    
open FSharp.QueryProvider.DataReader
open FSharp.QueryProvider.PreparedQuery
open FSharp.QueryProvider.Expression
open FSharp.QueryProvider.ExpressionMatching
open System.Linq.Expressions

open Microsoft.FSharp.Reflection

type internal IQueryable = System.Linq.IQueryable
type internal IQueryable<'T> = System.Linq.IQueryable<'T>

module List =
    /// <summary>
    /// Take a list of 'T ie `aaa` and a second list of `b` and insert between each `a`, `ababa`
    /// </summary>
    /// <param name="toInsert">List of 'T to insert between</param>
    /// <param name="list">List of 'T</param>
    let interpolate (toInsert : 'T list) (list : 'T list) : 'T list=
        list 
        |> List.fold(fun acum item -> 
            match acum with 
            | [] -> [item]
            | _ -> acum @ toInsert @ [item]
        ) []

/// <summary>
/// Context to be passed down when translating expressions.
/// </summary>
type Context = {
    TableAlias : string list option
    TopQuery : bool
}

/// <summary>
/// 
/// </summary>
type DBType<'t>= 
| Unhandled
| DataType of 't


/// <summary>
/// Wrap different sources of type.
/// </summary>
type TypeSource = 
| Method of System.Reflection.MethodInfo
| Property of System.Reflection.PropertyInfo
| Value of obj
| Type of System.Type

/// <summary>
/// Signarture for getting a DBType
/// </summary>
type GetDBType<'t> = TypeSource -> DBType<'t>
/// <summary>
/// Signature for getting a table name
/// </summary>
type GetTableName = System.Type -> string option
/// <summary>
/// Signature for getting a column name
/// </summary>
type GetColumnName = System.Reflection.MemberInfo -> string option

/// <summary>
/// Unwrap a method call expression and get its lambda argument.
/// </summary>
/// <param name="m"></param>
let getLambda (m : MethodCallExpression) =
        (stripQuotes (m.Arguments.Item(1))) :?> LambdaExpression

/// <summary>
/// Matches a lambda for `fun p -> p`
/// </summary>
/// <param name="l"></param>
let (|SingleSameSelect|_|) (l : LambdaExpression) =
    match l.Body with
    | :? ParameterExpression as paramAccess -> 
        let param = (l.Parameters |> Seq.exactlyOne)
        if l.Parameters.Count = 1 && paramAccess.Type = param.Type then
            Some param
        else 
            None
    | _ -> None

/// <summary>
/// shorthand for invoking a MethodCallExpression
/// </summary>
/// <param name="m"></param>
let invoke (m : MethodCallExpression) = 
    Expression.Lambda(m).Compile().DynamicInvoke()

/// <summary>
/// Gets a method by name from a list
/// </summary>
/// <param name="name">The name to match on</param>
/// <param name="ml"></param>
let getMethod name (ml : MethodCallExpression list) = 
    let m = ml |> List.tryFind(fun m -> m.Method.Name = name)
    match m with
    | Some m -> 
        Some(m), (ml |> List.filter(fun ms -> ms <> m))
    | None -> None, ml

/// <summary>
/// Gets all methods with a particular name
/// </summary>
/// <param name="names">List of nammes to match on</param>
/// <param name="ml"></param>
let getMethods names (ml : MethodCallExpression list) = 
    let methods = ml |> List.filter(fun m -> names |> List.exists(fun n -> m.Method.Name = n))
    methods, (ml |> List.filter(fun ms -> methods |> List.forall(fun m -> m <> ms)))

/// <summary>
/// Take a list&lt;'a list * 'b list * 'c list&gt; and concat a, b, and c no themseleves, producing a 'a list * 'b list * 'c list
/// </summary>
/// <param name="source"></param>
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

/// <summary>
/// Assert that a union case has exactly one field, then return its Reflection.PropertyInfo
/// </summary>
/// <param name="t"></param>
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

/// <summary>
/// Gets the underlying type for fsharp types
/// </summary>
/// <param name="t"></param>
let rec unwrapType (t : System.Type) = 
    if isOption t then
        t.GetGenericArguments() |> Seq.head |> unwrapType
    else if t |> FSharpType.IsUnion then
        (unionExactlyOneCaseOneField t).PropertyType |> unwrapType
    else
        t
/// <summary>
/// Create a prepared parameter
/// </summary>
/// <param name="columnIndex"></param>
/// <param name="value"></param>
/// <param name="dbType"></param>
let createParameter columnIndex value dbType = 
    {
        PreparedParameter.Name = "@p" + columnIndex.ToString()
        Value = value
        DbType = dbType
    }

/// <summary>
/// Get the actual value of an object. Unwraps options and unions
/// </summary>
/// <param name="value"></param>
let rec unwrapValue value = 
    let t = value.GetType()
    if t |> isOption then
        t.GetMethod("get_Value").Invoke(value, [||]) |> unwrapValue
    else if t |> FSharpType.IsUnion then
        t.GetMethod("get_Item").Invoke(value, [||]) |> unwrapValue
    else
        value

/// <summary>
/// Create query and parametter
/// </summary>
/// <param name="columnIndex"></param>
/// <param name="dbType"></param>
/// <param name="value"></param>
let rec valueToQueryAndParam (columnIndex : int) (dbType : DBType<_>) (value : obj)= 
    let value = unwrapValue value
    match dbType with
    | Unhandled -> failwithf "Unable to determine sql data type for type '%s'" (value.GetType().Name)
    | DataType t ->
        let p = (createParameter columnIndex value t)
        [p.Name], [p], List.empty<ConstructionInfo>
        

/// <summary>
/// Create a DbNull parameter for a type.
/// </summary>
/// <param name="columnIndex"></param>
/// <param name="dbType"></param>
let createNull (columnIndex : int) (dbType : DBType<_>) =
    match dbType with
    | Unhandled -> failwith "Shouldnt ever get to this point with this value" 
    | DataType dbType ->
        let p = createParameter columnIndex System.DBNull.Value dbType
        [p.Name], [p], List.empty<ConstructionInfo>

/// <summary>
/// Get all operations in a LINQ chain and the queryable they are operating on.
/// </summary>
/// <param name="e"></param>
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
        | _x ->
            if typedefof<IQueryable>.IsAssignableFrom e.Type then
                let r = invoke(e) :?> IQueryable
                let e = r.Expression
                match e with
                | Constant _ -> Some (Some (r), [])
                | Call m -> 
                    get m
                | _a -> failwith "shouldnt get here"
            else
                None

    let result = get e
    match result with 
    | None -> None
    | Some result -> 
        Some(fst(result).Value, snd(result))

open System.Reflection
/// <summary>
/// Take an expression for either a funciton or a member access chain and get the value.
/// </summary>
/// <param name="e"></param>
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