module internal FSharp.QueryProvider.QueryTranslatorUtilities
    
open FSharp.QueryProvider.Expression
open System.Linq.Expressions

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
    TopSelect : bool
}

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

let isOption (t : System.Type) = 
    t.IsGenericType &&
    t.GetGenericTypeDefinition() = typedefof<Option<_>>