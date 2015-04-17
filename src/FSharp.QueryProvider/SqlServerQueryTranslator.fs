namespace FSharp.QueryProvider.QueryTranslator

type PreparedParameter<'T> =
    {
        Name : string
        Value : obj 
        DbType : 'T
    }

type PreparedStatement<'P> =
    {
        Text : string
        FormattedText : string
        Parameters : PreparedParameter<'P> seq
    }

type IQueryable = System.Linq.IQueryable
type IQueryable<'T> = System.Linq.IQueryable<'T>

open System.Linq.Expressions
open FSharp.QueryProvider.Expression
open FSharp.QueryProvider.ExpressionMatching

module SqlServer =
    
    module private List =
            let interpolate (toInsert : 'T list) (list : 'T list) : 'T list=
                list 
                |> List.fold(fun acum item -> 
                    match acum with 
                    | [] -> [item]
                    | _ -> acum @ toInsert @ [item]
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

        let columnNameUnique = ref 0

        let getColumnNameIndex () = 
            columnNameUnique := (!columnNameUnique + 1)
            !columnNameUnique

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

//        let createSelect tableName columnList =
//                ["SELECT "] @ columnList @ [" FROM "; tableName]

        let getMethod name (ml : MethodCallExpression list) = 
            let m = ml |> List.tryFind(fun m -> m.Method.Name = name)
            match m with
            | Some m -> 
                Some(m), (ml |> List.filter(fun ms -> ms <> m))
            | None -> None, ml

        let getMethods names (ml : MethodCallExpression list) = 
            let methods = ml |> List.filter(fun m -> names |> List.exists(fun n -> m.Method.Name = n))
            methods, (ml |> List.filter(fun ms -> methods |> List.forall(fun m -> m <> ms)))

        let splitListOfTuples source = 
            source |> List.fold(fun a b ->
                fst(a) @ fst(b), snd(a) @ snd(b)
            ) (List.empty, List.empty)

        let rec mapFun e : ExpressionResult * (list<string> * list<PreparedParameter<_>>) = 
            let map = map(mapFun)
            let map2 = fun e ->
                map e |> splitListOfTuples
                
            let bin (e : BinaryExpression) (text : string) = 
                let leftSql, leftParams = map2(e.Left)
                let rightSql, rightParams = map2(e.Right)
                Some (leftSql @ [" "; text; " "] @ rightSql, leftParams @ rightParams)

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

            let valueToQueryAndParam (value : obj) = 
                let dbType = 
                    match System.Type.GetTypeCode(value.GetType()) with 
                    | System.TypeCode.Boolean -> System.Data.SqlDbType.Bit
                    | System.TypeCode.String -> System.Data.SqlDbType.NVarChar
                    | System.TypeCode.DateTime -> System.Data.SqlDbType.DateTime2
                    | System.TypeCode.Byte -> System.Data.SqlDbType.TinyInt
                    | System.TypeCode.Int16 -> System.Data.SqlDbType.SmallInt
                    | System.TypeCode.Int32 -> System.Data.SqlDbType.Int
                    | System.TypeCode.Int64 -> System.Data.SqlDbType.BigInt
                    | System.TypeCode.Object -> failwithf "The constant for '%A' is not supported" value
                    | t -> failwithf "not implemented type '%s'" (t.ToString())

                let p = {
                    PreparedParameter.Name = "p" + getColumnNameIndex().ToString()
                    Value = value
                    DbType = dbType
                }

                ["@"; p.Name; ""], [p]

            let result : option<string list * PreparedParameter<_> list> = 
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

                        let star = ["* "], []
                        let selectColumn, selectParameters = 
                            match count with
                            | Some c-> ["COUNT(*) "], []  //not fully implemented
                            | None -> 
                                match contains with 
                                | Some c -> 
                                    ["CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END "], []
                                | None -> 
                                    match maxOrMin with 
                                    | Some m ->
                                        let q, p = (getLambda(m).Body |> map2) 
                                        q @ [" "], p
                                    | None -> 
                                        match select with
                                        | Some s->
                                            match getLambda(s) with
                                            | SingleSameSelect _ -> star
                                            | l -> 
                                                let q, p = (getLambda(m).Body |> map2) 
                                                q @ [" "], p
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

                        let whereClause, whereParameters =
                            let fromWhere = 
                                match wheres with
                                | [] -> None
                                | _ -> 
                                    let wheres, parameters =
                                        wheres |> List.rev |> List.map(fun w ->
                                            let lambda = getLambda(w)
                                            let q, p = lambda.Body |> map2
                                            [q], p
                                        ) |> splitListOfTuples
                                    let sql = wheres |> List.interpolate [[" AND "]] |> List.reduce(@)
                                    Some (sql, parameters)
                            let fromContains =
                                match contains with
                                | Some c -> 
                                    let x, xp = getLambda(select.Value).Body |> map2
                                    let y, yp= c.Arguments.Item(1) |> map2
                                    Some (x @ [" = "] @ y, xp @ yp)
                                | None -> None

                            let total = 
                                match fromWhere, fromContains with
                                | None, None -> None
                                | Some w, None -> Some w
                                | None, Some c -> Some c
                                | Some(w,wp), Some(c,cp) -> Some(w @ [" AND "] @ c, wp @ cp)

                            match total with 
                            | None -> [], []
                            | Some (q, qp) -> [" WHERE ("] @ q @ [")"], qp

                        let orderByClause, orderByParameters =
                            match sorts with
                            | [] -> [], []
                            | _ ->
                                let colSorts, parameters = 
                                    sorts |> List.rev |> List.map(fun s ->
                                        let sortMethod = 
                                            match s.Method.Name with
                                            | "Min" | "OrderBy" | "ThenBy" -> "ASC"
                                            | "Max" | "OrderByDescending" | "ThenByDescending" -> "DESC"
                                            | n -> failwithf "Sort methods not implemented '%s'" n
                                        let lambda = getLambda(s)
                                        let sql, parameters = (lambda.Body |> map2)
                                        [sql @ [" "; sortMethod]], parameters
                                    ) |> splitListOfTuples

                                //let colSorts = colSorts |> List.interpolate [", "]
                                let colSorts = colSorts |> List.interpolate [[", "]] |> List.reduce(@)

                                [" ORDER BY "] @ colSorts, parameters

                        let sql = selectStatement @ whereClause @ orderByClause
                        let parameters = orderByParameters @ whereParameters
                        Some (sql, parameters)
                    | None ->
                        match m.Method.Name with
                        | "Contains" | "StartsWith" | "EndsWith" as typeName when(m.Object.Type = typedefof<string>) ->
                            let valQ, valP = valueToQueryAndParam ((arg :?> ConstantExpression).Value)
                            let search =
                                match typeName with 
                                | "Contains" -> ["'%' + "] @ valQ @ [" + '%'"]
                                | "StartsWith" -> valQ @ [" + '%'"]
                                | "EndsWith" -> ["'%' + "] @ valQ
                                | _ -> failwithf "not implemented %s" typeName
                            let colQ, colP = map2(m.Object)

                            Some (colQ @ [" LIKE "] @ search, colP @ valP)
                        | "Invoke" -> 
                            let result = Expression.Lambda(m).Compile().DynamicInvoke()
                            Some (valueToQueryAndParam(result))
                        | x -> failwithf "Method '%s' is not implemented." x
                | Not n ->
                    let sql, parameters = map2(n.Operand)
                    Some ([" NOT "] @ sql, parameters) 
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
                        Some (["NULL"] ,[])
                    else
                        Some (valueToQueryAndParam c.Value)
                | MemberAccess m ->
                    if m.Expression <> null && m.Expression.NodeType = ExpressionType.Parameter then
                        Some ([m.Member.Name], [])
                    else
                        failwithf "The member '%s' is not supported" m.Member.Name
                | _ -> None

            match result with
            | Some r -> ExpressionResult.Skip, r
            | None -> ExpressionResult.Recurse, ([], [])
//            if result.IsSome then 
//                ExpressionResult.Skip, result
//            else
//                ExpressionResult.Recurse, result

        let results = 
            expression
            |> map(mapFun)

        let queryList, queryParameters =  results |> splitListOfTuples

        let query = queryList |> String.concat("")

        {
            PreparedStatement.Text = query
            FormattedText = query
            Parameters = queryParameters //Seq.empty<PreparedParameter<System.Data.SqlDbType>>
        }