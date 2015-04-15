namespace FSharp.QueryProvider.QueryTranslator

type PreparedParameter =
    {
        Name : string
        Value : obj
    }

type PreparedStatement =
    {
        Text : string
        FormattedText : string
        Parameters : PreparedParameter seq
    }

type IQueryable = System.Linq.IQueryable
type IQueryable<'T> = System.Linq.IQueryable<'T>

open System.Linq.Expressions
open FSharp.QueryProvider.Expression
open FSharp.QueryProvider.ExpressionMatching

module SqlServer =
    
    let translate (expression : Expression) = 
        
//        let mutable tableAliases = Map.empty<Expression, string>
//        let mutable tableAliasIndex = 0
//
//        let getTableAlias (table : 't IQueryable) : string = 
//            let existing = tableAliases |> Map.tryFind table
//            if existing.IsNone then
//                tableAliasIndex <- (tableAliasIndex + 1)
//                let created = "T" + tableAliasIndex.ToString
//                tableAliases |> Map.add table created
//            else
//                existing.Value
//            //if existing.
        let createSelect tableName columnList =
                ["SELECT "] @ columnList @ [" FROM "; tableName]
        let getMethod name (ml : MethodCallExpression list) = 
            let m = ml |> List.tryFind(fun m -> m.Method.Name = name)
            match m with
            | Some m -> 
                Some(m), (ml |> List.filter(fun ms -> ms <> m))
            | None -> None, ml
            
        let getBody (m : MethodCallExpression) =
            (stripQuotes (m.Arguments.Item(1))) :?> LambdaExpression

        let rec mapFun e : ExpressionResult * list<string> option = 
            let map = map(mapFun)
            let bin (e : BinaryExpression) text = 
                Some (map(e.Left) @ [" "; text; " "] @ map(e.Right))

            let getOperationsAndQueryable e : option<IQueryable * MethodCallExpression list> =
                let rec get (e : MethodCallExpression) : option<IQueryable option * MethodCallExpression list> = 
                    let paramCount = e.Arguments.Count
                    if paramCount = 0 then
                        None
                    else
                        let first = (e.Arguments |> Seq.head)
                        if typedefof<IQueryable>.IsAssignableFrom first.Type then
                            match first with
                            | Constant c -> Some(Some(c.Value :?> IQueryable), [e])
                            | Call m -> 
                                let r = get(m)
                                match r with 
                                | Some r -> 
                                    Some (fst(r), snd(r) |> List.append([e]))
                                | None -> None
                            | x -> failwithf "not implemented nodetype '%A'" first.NodeType
                        else
                            None

                let result = get e
                match result with 
                | None -> None
                | Some result -> 
                    Some(fst(result).Value, snd(result))

            let result = 
                match e with
                | Call m -> 
                    let arg = m.Arguments.Item(0) 
                    let linqChain = getOperationsAndQueryable m

                    match linqChain with
                    | Some (queryable, ml) ->
                        //let getMethod name = getMethod name methods

                        let select, ml = getMethod "Select" ml
                        let where, ml = getMethod "Where" ml
                        let count, ml = getMethod "Count" ml
                        let last, ml = getMethod "Last" ml
                        let lastOrDefault, ml= getMethod "LastOrDefault" ml

                        if ml |> Seq.length > 0 then 
                            let methodNames = (ml |> Seq.map(fun m -> sprintf "'%s'" m.Method.Name) |> String.concat(","))
                            failwithf "Methods not implemented: %s" methodNames

                        if last.IsSome then
                            failwith "'last' operator has no translations for Sql Server"
                        if lastOrDefault.IsSome then
                            failwith "'lastOrDefault' operator has no translations for Sql Server"

                        let star = ["*"]
                        let selectColumn = 
                            match count with
                            | Some c-> ["COUNT(*)"]  //not fully implemented
                            | None -> 
                                match select with
                                | Some s->
                                    let lambda = getBody(s)
                                    match lambda.Body with 
                                    | :? ParameterExpression as paramAccess -> 
                                        let param = (lambda.Parameters |> Seq.exactlyOne)
                                        if lambda.Parameters.Count = 1 && paramAccess.Type = param.Type then
                                            star
                                        else 
                                            failwithf "unexpected parameter type '%s'" param.Type.Name
                                    | _ -> lambda.Body |> map
                                | None -> star
                        let selectStatement =
                            let table = queryable.ElementType.Name
                            ["SELECT " ] @ selectColumn @ [" FROM "; table]

                        let whereClause =
                            match where with
                            | Some w-> 
                                //let select = map arg
                                let lambda = getBody(w)
                                let where = lambda.Body |> map
                                [" WHERE ("] @ where @ [")"]
                            | None -> []

                        Some (selectStatement @ whereClause)
                    | None ->
                        match m.Method.Name with
                        | "Contains" | "StartsWith" | "EndsWith" as typeName when(m.Object.Type = typedefof<string>) ->
                            let value = (arg :?> ConstantExpression).Value :?> string
                            let arg =
                                match typeName with 
                                | "Contains" -> ["%"; value; "%"]
                                | "StartsWith" -> ["%"; value]
                                | "EndsWith" -> [value; "%"]
                                | _ -> failwithf "not implemented %s" typeName
                            Some (map(m.Object) @ [" LIKE '"] @ arg @ ["'"])
                        | x -> failwithf "Method '%s' is not implemented." x

//                    match m.Method.Name with
//                    | "Select" | "Where" ->
//                        //let argResult = arg |> map
//                        let lambda = (stripQuotes (m.Arguments.Item(1))) :?> LambdaExpression
//                        let result = 
//                            match m.Method.Name with
//                            | "Select" -> 
//                                let tableName = ((arg :?> ConstantExpression).Value :?> IQueryable).ElementType.Name
//
//                                let selectList = 
//                                    match lambda.Body with 
//                                    | :? ParameterExpression as paramAccess -> 
//                                        let param = (lambda.Parameters |> Seq.exactlyOne)
//                                        if lambda.Parameters.Count = 1 && paramAccess.Type = param.Type then
//                                            ["*"]
//                                        else 
//                                            failwithf "unexpected parameter type '%s'" param.Type.Name
//                                    | _ -> lambda.Body |> map
//                                createSelect tableName selectList
//                            | "Where" -> 
//                                let select = map arg
//                                let where = lambda.Body |> map
//                                select @ [" WHERE ("] @ where @ [")"]
//                            | x -> failwithf "Method '%s' is not implemented." x
//                        Some result
//                    | "Contains" | "StartsWith" | "EndsWith" as typeName when(m.Object.Type = typedefof<string>) ->
//                        let value = (arg :?> ConstantExpression).Value :?> string
//                        let arg =
//                            match typeName with 
//                            | "Contains" -> ["%"; value; "%"]
//                            | "StartsWith" -> ["%"; value]
//                            | "EndsWith" -> [value; "%"]
//                            | _ -> failwithf "not implemented %s" typeName
//                        Some (map(m.Object) @ [" LIKE '"] @ arg @ ["'"])
//                    | x -> failwithf "Method '%s' is not implemented." x
                | Not n ->
                    Some ([" NOT "] @ map(n.Operand))
                | And e -> bin e "AND"
                | AndAlso e -> bin e "AND"
                | Or e -> bin e "OR"
                | OrElse e -> bin e "OR"
                | Equal e -> bin e "="
                | NotEqual e -> bin e "<>"
                | LessThan e -> bin e "<"
                | LessThanOrEqual e -> bin e "<="
                | GreaterThan e -> bin e ">"
                | GreaterThanOrEqual e -> bin e ">="
                | Constant c ->
                    let q = 
                        match c.Value with 
                        | :? IQueryable as v -> Some v
                        | _ -> None
                    if q <> None then
                        let q = q.Value
                        failwith "dont think this should ever get hit"
                        //Some ["SELECT * FROM "; q.ElementType.Name]
                    else if c.Value = null then
                        Some ["NULL"]
                    else
                        let cv = 
                            match System.Type.GetTypeCode(c.Value.GetType()) with 
                            | System.TypeCode.Boolean -> if (c.Value :?> bool) then "1" else "0"
                            | System.TypeCode.String -> "'" + (c.Value :?> string) + "'"
                            | System.TypeCode.Object -> failwithf "The constant for '%A' is not supported" c.Value
                            | _ -> c.Value.ToString()
                        Some [cv]
                | MemberAccess m ->
                    if m.Expression <> null && m.Expression.NodeType = ExpressionType.Parameter then
                        Some [m.Member.Name]
                    else
                        failwithf "The member '%s' is not supported" m.Member.Name
                | _ -> None
            if result.IsSome then 
                ExpressionResult.Skip, result
            else
                ExpressionResult.Recurse, result

        let ls = 
            expression
            |> map(mapFun)

        let query = 
            ls 
            |> String.concat("")

        {
            PreparedStatement.Text = query
            FormattedText = query
            Parameters = []
        }