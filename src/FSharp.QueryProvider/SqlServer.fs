namespace FSharp.QueryProvider.Engines

type IQueryable = System.Linq.IQueryable
type IQueryable<'T> = System.Linq.IQueryable<'T>
type SqlDbType = System.Data.SqlDbType

open System.Linq.Expressions
open FSharp.QueryProvider
open FSharp.QueryProvider.Expression
open FSharp.QueryProvider.ExpressionMatching
open FSharp.QueryProvider.QueryTranslatorUtilities
open FSharp.QueryProvider.DataReader
open FSharp.QueryProvider.PreparedQuery

module SqlServer =

    let defaultGetDBType (morP : TypeSource) : SqlDbType DBType =
        let t = 
            match morP with 
            | Method m -> m.ReturnType
            | Property p -> p.PropertyType
            | Value v -> v.GetType()
            | Type t -> t
        let t = unwrapType t
        match System.Type.GetTypeCode(t) with 
        | System.TypeCode.Boolean -> DataType SqlDbType.Bit
        | System.TypeCode.String -> DataType SqlDbType.NVarChar
        | System.TypeCode.DateTime -> DataType SqlDbType.DateTime2
        | System.TypeCode.Byte -> DataType SqlDbType.TinyInt
        | System.TypeCode.Int16 -> DataType SqlDbType.SmallInt
        | System.TypeCode.Int32 -> DataType SqlDbType.Int
        | System.TypeCode.Int64 -> DataType SqlDbType.BigInt
        | t -> Unhandled

    let defaultGetTableName (t:System.Type) : string =
        t.Name
    let defaultGetColumnName (t:System.Reflection.MemberInfo) : string =
        t.Name

    //terible duplication of code between this and createTypeSelect. needs to be refactored.
    let createTypeConstructionInfo selectIndex (t : System.Type) manyOrOne =
        if Microsoft.FSharp.Reflection.FSharpType.IsRecord t then
            let fields = Microsoft.FSharp.Reflection.FSharpType.GetRecordFields t |> Seq.toList

            let ctorArgs = fields |> Seq.mapi(fun i f ->
                selectIndex + i
            )

            {
                ManyOrOne = manyOrOne
                Type = t
                ConstructorArgs = ctorArgs
                PropertySets = []
            }
        else
            let x = typedefof<int>
            let simple () = 
                {
                    ManyOrOne = manyOrOne
                    Type = t
                    ConstructorArgs = [selectIndex] 
                    PropertySets = []
                }
            let simpleTypes = [
                typedefof<int16>
                typedefof<int>
                typedefof<int64>
                typedefof<float>
                typedefof<decimal>
                typedefof<string>
                typedefof<System.DateTime>
                typedefof<bool>
            ]
            if simpleTypes |> List.exists ((=) t) then
                simple()
            else
                failwith "not implemented type '%s'" t.Name

    //terible duplication of code between this and createTypeSelect. needs to be refactored.
    let createTypeSelect (getColumnName : System.Reflection.MemberInfo -> string) (tableAlias : string list) (topSelect : bool) (t : System.Type) =
        // need to call a function here so that this can be extended
        if Microsoft.FSharp.Reflection.FSharpType.IsRecord t then
            let fields = Microsoft.FSharp.Reflection.FSharpType.GetRecordFields t |> Seq.toList

            let query = 
                fields 
                |> List.map(fun  f -> tableAlias @ ["."; getColumnName f]) 
                |> List.interpolate([[", "]])
                |> List.reduce(@)

            let query = query @ [" "]

            let ctor = 
                if topSelect then
                    Some (createTypeConstructionInfo 0 t Many)
                else
                    None

            query, ctor
        else
            failwith "not implemented, only records are currently implemented"

    let translate 
        (getDBType : GetDBType<SqlDbType> option) 
        (getTableName : GetTableName option) 
        (getColumnName : GetColumnName option) 
        (expression : Expression) = 
        
        let getDBType = 
            match getDBType with 
            | Some g -> fun morP -> 
                match g morP with
                | Unhandled -> 
                    match defaultGetDBType morP with
                    | Unhandled -> failwithf "Could not determine DataType for '%A' is not handled" morP
                    | r -> r
                | r -> r
            | None -> defaultGetDBType

        let getColumnName =
            match getColumnName with
            | Some g -> fun t ->
                match g t with 
                | Some r ->  r
                | None -> defaultGetColumnName t
            | None -> defaultGetColumnName
        let getTableName =
            match getTableName with
            | Some g -> fun t ->
                match g t with 
                | Some r ->  r
                | None -> defaultGetTableName t
            | None -> defaultGetTableName

        let columnNameUnique = ref 0

        let getColumnNameIndex () = 
            columnNameUnique := (!columnNameUnique + 1)
            !columnNameUnique

        let tableAliasIndex = ref 1

        let getTableAlias () = 
            let a = 
                match !tableAliasIndex with
                | 1 -> ["T"]
                | i -> ["T"; i.ToString()]
            tableAliasIndex := (!tableAliasIndex + 1)
            a
        
        let rec mapFun (context : Context) e : ExpressionResult * (string list * PreparedParameter<_> list * TypeConstructionInfo list) = 
            let mapd = fun context e ->
                mapd(mapFun) context e |> splitResults
            let map = fun e ->
                mapd context e

            let valueToQueryAndParam dbType value = 
                valueToQueryAndParam (getColumnNameIndex()) dbType value
            let createNull dbType = 
                createNull (getColumnNameIndex()) dbType

            let createTypeSelect tableAlias t = 
                let q, ctorOption = createTypeSelect getColumnName tableAlias context.TopSelect t
                let c = 
                    match ctorOption with
                    | None -> []
                    | Some c -> [c]
                q, [], c

            let bin (e : BinaryExpression) (text : string) = 
                let leftSql, leftParams, leftCtor = map(e.Left)
                let rightSql, rightParams, rightCtor = map(e.Right)
                Some (leftSql @ [" "; text; " "] @ rightSql, leftParams @ rightParams, leftCtor @ rightCtor)

            let getOperationsAndQueryable e : option<IQueryable * MethodCallExpression list> =
                let rec get (e : MethodCallExpression) : option<IQueryable option * MethodCallExpression list> = 
                    match e with
                    | CallIQueryable(e, q, args) -> 
                        match q with
                        | Constant c -> Some(Some(c.Value :?> IQueryable), [e])
                        | Call m -> 
                            let r = get(m)
                            match r with 
                            | Some r -> 
                                Some (fst(r), snd(r) |> List.append([e]))
                            | None -> None
                        | x -> failwithf "not implemented nodetype '%A'" q.NodeType
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

            let result : option<string list * PreparedParameter<_> list * TypeConstructionInfo list>= 
                match e with
                | Call m -> 
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

                        let tableAlias = (getTableAlias())
                        let map e = 
                            mapd {TableAlias = Some tableAlias; TopSelect = false} e
                        let createTypeSelect t =
                            createTypeSelect tableAlias t

                        let star = ["* "], []
                        let selectColumn, selectParameters, selectCtor = 
                            match count with
                            | Some c-> 
                                ["COUNT(*) "], [], [createTypeConstructionInfo 0 typedefof<int> One]
                            | None -> 
                                match contains with 
                                | Some c -> 
                                    ["CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END "], [] , [createTypeConstructionInfo 0 typedefof<bool> One]
                                | None -> 
                                    let partialSelect (l : LambdaExpression) =
                                        let t = l.ReturnType
                                        let c = 
                                            if context.TopSelect then
                                                [createTypeConstructionInfo 0 t Many]
                                            else
                                                []
                                        let q, p, _ = (getLambda(m).Body |> map) 
                                        q @ [" "], p, c
                                    match maxOrMin with 
                                    | Some m ->
                                        partialSelect (getLambda m)
                                    | None -> 
                                        match select with
                                        | Some s->
                                            match getLambda(s) with
                                            | SingleSameSelect x -> 
                                                createTypeSelect x.Type
                                            | l ->
                                                partialSelect l
                                        | None -> 
                                            createTypeSelect (Queryable.TypeSystem.getElementType (queryable.GetType()))
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
                            ["SELECT " ] @ topStatement @ selectColumn @ ["FROM "; getTableName(queryable.ElementType); " AS "; ] @ tableAlias

                        let whereClause, whereParameters, whereCtor =
                            let fromWhere = 
                                match wheres with
                                | [] -> None
                                | _ -> 
                                    let wheres, parameters, ctors =
                                        wheres |> List.rev |> List.map(fun w ->
                                            let b = getLambda(w).Body
                                            let x = 
                                                match b with 
                                                | Call m when(m.Method.Name = "Contains") ->
                                                    //(PersonId IN (SELECT PersonID FROM Employee))
                                                    match m with 
                                                    | CallIQueryable(m,q,rest) ->  
                                                        let containsVal = rest |> Seq.head
                                                        match containsVal with
                                                        | MemberAccess a -> 
                                                            let accessQ, accessP, accessCtor = a |> map
                                                            let subQ, subP, subCtor = q |> map
                                                            Some ([accessQ @ [" IN ("] @ subQ @ [")"]], accessP @ subP, accessCtor @ subCtor)
                                                        | _ -> 
                                                            None
                                                    | _ -> None
                                                | b -> None
                                            match x with
                                            | None -> 
                                                let q, p, c = b |> map
                                                [q], p, c
                                            | Some x ->
                                                x
                                        ) |> splitResults
                                    let sql = wheres |> List.interpolate [[" AND "]] |> List.reduce(@)
                                    Some (sql, parameters, ctors)

                            let fromContains =
                                match contains with
                                | Some c -> 
                                    let xq, xp, xc = getLambda(select.Value).Body |> map
                                    let yq, yp, yc = c.Arguments.Item(1) |> map
                                    Some (xq @ [" = "] @ yq, xp @ yp, xc @ yc)
                                | None -> None

                            let total = 
                                match fromWhere, fromContains with
                                | None, None -> None
                                | Some w, None -> Some w
                                | None, Some c -> Some c
                                | Some(wq,wp,wc), Some(cq,cp,cc) -> Some(wq @ [" AND "] @ cq, wp @ cp, wc @ cc)

                            match total with 
                            | None -> [], [], []
                            | Some (q, qp, qc) -> [" WHERE ("] @ q @ [")"], qp, qc

                        let orderByClause, orderByParameters, orderByCtor =
                            match sorts with
                            | [] -> [], [], []
                            | _ ->
                                let colSorts, parameters, ctor = 
                                    sorts |> List.rev |> List.map(fun s ->
                                        let sortMethod = 
                                            match s.Method.Name with
                                            | "Min" | "OrderBy" | "ThenBy" -> "ASC"
                                            | "Max" | "OrderByDescending" | "ThenByDescending" -> "DESC"
                                            | n -> failwithf "Sort methods not implemented '%s'" n
                                        let lambda = getLambda(s)
                                        let sql, parameters, ctor = (lambda.Body |> map)
                                        [sql @ [" "; sortMethod]], parameters, ctor
                                    ) |> splitResults

                                //let colSorts = colSorts |> List.interpolate [", "]
                                let colSorts = colSorts |> List.interpolate [[", "]] |> List.reduce(@)

                                [" ORDER BY "] @ colSorts, parameters, ctor

                        let sql = selectStatement @ whereClause @ orderByClause
                        let parameters = orderByParameters @ whereParameters @ selectParameters
                        let ctor = orderByCtor @ whereCtor @ selectCtor
                        Some (sql, parameters, ctor)
                    | None ->
                        let simpleInvoke m = 
                            let v = invoke m
                            Some (v |> valueToQueryAndParam (getDBType (Value v)))

                        match m.Method.Name with
                        | "Contains" | "StartsWith" | "EndsWith" as typeName when(m.Object.Type = typedefof<string>) ->
                            let value = (m.Arguments.Item(0)  :?> ConstantExpression).Value
                            let valQ, valP, valC = valueToQueryAndParam (getDBType (Value value)) value
                            let search =
                                match typeName with 
                                | "Contains" -> ["'%' + "] @ valQ @ [" + '%'"]
                                | "StartsWith" -> valQ @ [" + '%'"]
                                | "EndsWith" -> ["'%' + "] @ valQ
                                | _ -> failwithf "not implemented %s" typeName
                            let colQ, colP, colC = map(m.Object)

                            Some (colQ @ [" LIKE "] @ search, colP @ valP, colC @ valC)
                        | "Invoke" | "op_Dereference" -> 
                            simpleInvoke m
                        | "Some" when (isOption m.Method.ReturnType) -> 
                            simpleInvoke m
                        | "get_None" when (isOption m.Method.ReturnType) -> 
                            let t = m.Method.ReturnType.GetGenericArguments() |> Seq.head
                            Some (createNull (getDBType (Type t)))
                        | x -> failwithf "Method '%s' is not implemented." x
                | Not n ->
                    let sql, parameters, ctor = map(n.Operand)
                    Some ([" NOT "] @ sql, parameters, ctor) 
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
                        Some (["NULL"] ,[], [])
                    else
                        Some (valueToQueryAndParam (getDBType (Value c.Value)) c.Value)
                | MemberAccess m ->
//                    failwith "should be handled explicitly"
                    if m.Expression <> null && m.Expression.NodeType = ExpressionType.Parameter then
                        //this needs to call a function to determine column name for extensibility.    
                        match context.TableAlias with
                        | Some tableAlias -> Some (tableAlias @ ["."; getColumnName(m.Member)], [], [])
                        | None -> failwith "cannot access member without tablealias being genned"
                    else
                        failwithf "The member '%s' is not supported" m.Member.Name
                | _ -> None

            match result with
            | Some r -> ExpressionResult.Skip, r
            | None -> ExpressionResult.Recurse, ([], [], [])

        let results = 
            expression
            |> mapd(mapFun) ({TableAlias = None; TopSelect = true})

        let queryList, queryParameters, resultConstructionInfo = results |> splitResults

        let query = queryList |> String.concat("")

        {
            PreparedStatement.Text = query
            FormattedText = query
            Parameters = queryParameters
            ResultConstructionInfo = resultConstructionInfo |> Seq.exactlyOne
        }

    let createCommand (connection : System.Data.SqlClient.SqlConnection) (preparedStatement : PreparedStatement<SqlDbType>) =
        
        let cmd = connection.CreateCommand()
        cmd.CommandText <- preparedStatement.Text
        for param in preparedStatement.Parameters do
            let sqlParam = cmd.CreateParameter()
            sqlParam.ParameterName <- param.Name
            sqlParam.Value <- param.Value
            sqlParam.SqlDbType <- param.DbType
            cmd.Parameters.Add(sqlParam) |> ignore
        cmd

    let translateToCommand getDBType getTableName getColumnName connection expression =
        let ps = translate getDBType getTableName getColumnName expression

        let cmd = createCommand connection ps

        cmd, ps.ResultConstructionInfo