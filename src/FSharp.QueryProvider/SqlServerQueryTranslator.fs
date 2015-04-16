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
    
    module private List =
            let interpolate (toInsert : 'T) (list : 'T list) : 'T list=
                list 
                |> List.fold(fun acum ls -> 
                    match acum with 
                    | [] -> [ls]
                    | _ -> acum @ [toInsert; ls]
                ) []

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

        let createSelect tableName columnList =
                ["SELECT "] @ columnList @ [" FROM "; tableName]

        let getMethod name (ml : MethodCallExpression list) = 
            let m = ml |> List.tryFind(fun m -> m.Method.Name = name)
            match m with
            | Some m -> 
                Some(m), (ml |> List.filter(fun ms -> ms <> m))
            | None -> None, ml

        let getMethods names (ml : MethodCallExpression list) = 
            let methods = ml |> List.filter(fun m -> names |> List.exists(fun n -> m.Method.Name = n))
            methods, (ml |> List.filter(fun ms -> methods |> List.forall(fun m -> m <> ms)))

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
                        let wheres, ml = getMethods ["Where"] ml
                        let count, ml = getMethod "Count" ml
                        let last, ml = getMethod "Last" ml
                        let lastOrDefault, ml= getMethod "LastOrDefault" ml
                        let contains, ml = getMethod "Contains" ml
                        let single, ml = getMethod "Single" ml
                        let singleOrDefault, ml = getMethod "SingleOrDefault" ml
                        let first, ml = getMethod "First" ml
                        let firstOrDefault, ml = getMethod "FirstOrDefault" ml
                        let max, ml = getMethod "Max" ml
                        let min, ml = getMethod "Min" ml
                        
                        let sorts, ml= getMethods ["OrderBy"; "OrderByDescending"; "ThenBy"; "ThenByDescending"] ml
                        let sorts, maxOrMin = 
                            let m = 
                                match max,min with
                                | Some _, Some _ -> failwith "invalid"
                                | Some m, None -> Some(m)
                                | None, Some m -> Some(m)
                                | None, None -> None
                            match m with
                            | Some _ -> sorts @ [m.Value], m
                            | None -> sorts, m

                        if ml |> Seq.length > 0 then 
                            let methodNames = (ml |> Seq.map(fun m -> sprintf "'%s'" m.Method.Name) |> String.concat(","))
                            failwithf "Methods not implemented: %s" methodNames

                        if last.IsSome then
                            failwith "'last' operator has no translations for Sql Server"
                        if lastOrDefault.IsSome then
                            failwith "'lastOrDefault' operator has no translations for Sql Server"

                        let star = ["* "]
                        let selectColumn = 
                            match count with
                            | Some c-> ["COUNT(*) "]  //not fully implemented
                            | None -> 
                                match contains with 
                                | Some c -> 
                                    ["CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END "]
                                | None -> 
                                    match maxOrMin with 
                                    | Some m ->
                                        (getLambda(m).Body |> map) @ [" "]
                                    | None -> 
                                        match select with
                                        | Some s->
                                            match getLambda(s) with
                                            | SingleSameSelect _ -> star
                                            | l -> (l.Body |> map) @ [" "]
                                        | None -> star

                        let topStatement = 
                            let count = 
                                if single.IsSome || singleOrDefault.IsSome then
                                    Some 2
                                else if 
                                    first.IsSome ||
                                    firstOrDefault.IsSome ||
                                    max.IsSome || 
                                    min.IsSome then
                                    Some 1
                                else
                                    None

                            match count with
                            | Some i -> ["TOP "; i.ToString(); " "]
                            | None -> []

                        let selectStatement =
                            let table = queryable.ElementType.Name
                            ["SELECT " ] @ topStatement @ selectColumn @ ["FROM "; table]

                        let whereClause =
                            let fromWhere = 
                                match wheres with
                                | [] -> []
                                | _ -> 
                                    wheres |> List.rev |> List.map(fun w ->
                                        let lambda = getLambda(w)
                                        lambda.Body |> map
                                    ) |> List.interpolate [" AND "] |> List.concat
                                    
                            let fromContains =
                                match contains with
                                | Some c -> 
                                    let x = getLambda(select.Value).Body |> map
                                    let y = c.Arguments.Item(1) |> map
                                    x @ [" = "] @ y
                                | None -> []
                            let total = 
                                match fromWhere, fromContains with
                                | [], [] -> []
                                | _, [] -> fromWhere
                                | [], _ -> fromContains
                                | _, _ -> fromWhere @ [" AND "] @ fromContains

                            match total with 
                            | [] -> []
                            | _ -> [" WHERE ("] @ total @ [")"]

                        let orderByClause =
                            match sorts with
                            | [] -> []
                            | _ ->
                                let colSorts = 
                                    sorts |> List.rev |> List.map(fun s ->
                                        let sortMethod = 
                                            match s.Method.Name with
                                            | "Min" | "OrderBy" | "ThenBy" -> "ASC"
                                            | "Max" | "OrderByDescending" | "ThenByDescending" -> "DESC"
                                            | n -> failwithf "Sort methods not implemented '%s'" n
                                        let lambda = getLambda(s)
                                        (lambda.Body |> map) @ [" "; sortMethod]
                                    ) |> List.interpolate [", "] |> List.concat

                                [" ORDER BY "] @ colSorts

                        Some (selectStatement @ whereClause @ orderByClause)
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
                            | System.TypeCode.String -> (c.Value :?> string)
                            | System.TypeCode.Object -> failwithf "The constant for '%A' is not supported" c.Value
                            | _ -> c.Value.ToString()
                        Some ["'"; cv; "'"]
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