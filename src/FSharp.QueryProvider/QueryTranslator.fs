namespace FSharp.QueryProvider

type SqlDbType = System.Data.SqlDbType

open System.Linq.Expressions
open FSharp.QueryProvider
open FSharp.QueryProvider.Expression
open FSharp.QueryProvider.ExpressionMatching
open FSharp.QueryProvider.QueryTranslatorUtilities
open FSharp.QueryProvider.DataReader
open FSharp.QueryProvider.PreparedQuery

module QueryTranslator =

    type QueryDialect = 
    | SqlServer2012

    type QueryType =
    | SelectQuery
    | DeleteQuery

    let private defaultGetDBType (morP : TypeSource) : SqlDbType DBType =
        let t = 
            match morP with 
            | Method m -> m.ReturnType
            | Property p -> p.PropertyType
            | TypeSource.Value v -> v.GetType()
            | TypeSource.Type t -> t
        let t = unwrapType t
        match System.Type.GetTypeCode(t) with 
        | System.TypeCode.Boolean -> DataType SqlDbType.Bit
        | System.TypeCode.String -> DataType SqlDbType.NVarChar
        | System.TypeCode.DateTime -> DataType SqlDbType.DateTime2
        | System.TypeCode.Byte -> DataType SqlDbType.TinyInt
        | System.TypeCode.Int16 -> DataType SqlDbType.SmallInt
        | System.TypeCode.Int32 -> DataType SqlDbType.Int
        | System.TypeCode.Int64 -> DataType SqlDbType.BigInt
        | _t -> Unhandled

    let private defaultGetTableName (t:System.Type) : string =
        t.Name
    let private defaultGetColumnName (t:System.Reflection.MemberInfo) : string =
        t.Name

    //terible duplication of code between this and createTypeSelect. needs to be refactored.
    let private createTypeConstructionInfo selectIndex (t : System.Type) : TypeConstructionInfo =
        if isValueType t then
            {
                Type = t
                ConstructorArgs = [Value selectIndex] 
                PropertySets = []
            }
        else
            if Microsoft.FSharp.Reflection.FSharpType.IsRecord t then
                let fields = Microsoft.FSharp.Reflection.FSharpType.GetRecordFields t |> Seq.toList

                let ctorArgs = fields |> Seq.mapi(fun i f ->
                    let selectIndex = (selectIndex + i)
                    let t = f.PropertyType
                    if isValueType t then
                        Value selectIndex
                    else if isOption t then
                        Type {
                            Type = t
                            ConstructorArgs = [Value selectIndex] 
                            PropertySets = []
                        }
                    else
                        failwith "non value type not supported"
                )
                
                {
                    Type = t
                    ConstructorArgs = ctorArgs
                    PropertySets = []
                }
                
            else
                failwithf "not implemented type '%s'" t.Name

    let private createConstructionInfoForType selectIndex (t : System.Type) returnType : ConstructionInfo =
        let typeCtor = createTypeConstructionInfo selectIndex t 
        {
            ReturnType = returnType
            Type = typeCtor.Type
            TypeOrLambda = TypeOrLambdaConstructionInfo.Type typeCtor
        }

    //terible duplication of code between this and createTypeSelect. needs to be refactored.
    let private createTypeSelect 
        (getColumnName : System.Reflection.MemberInfo -> string) 
        (tableAlias : string list) 
        (topQuery : bool) 
        (t : System.Type) =
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
                if topQuery then
                    Some (createTypeConstructionInfo 0 t)
//                    let typeCtor = createTypeConstructionInfo 0 t
//                    Some {
//                        ReturnType = returnType
//                        Type = typeCtor.Type
//                        TypeOrLambda = TypeOrLambdaConstructionInfo.Type typeCtor
//                    }
                else
                    None

            query, ctor
        else
            failwith "not implemented, only records are currently implemented"

    let translate 
        (_queryDialect : QueryDialect)
        (queryType : QueryType)
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
        
        let rec mapFun (context : Context) e : ExpressionResult * (string list * PreparedParameter<_> list * ConstructionInfo list) = 
            let mapd = fun context e ->
                mapd(mapFun) context e |> splitResults
            let map = fun e ->
                mapd context e

            let valueToQueryAndParam dbType value = 
                valueToQueryAndParam (getColumnNameIndex()) dbType value
            let createNull dbType = 
                createNull (getColumnNameIndex()) dbType

            let bin (e : BinaryExpression) (text : string) = 
                let leftSql, leftParams, leftCtor = map(e.Left)
                let rightSql, rightParams, rightCtor = map(e.Right)
                Some (leftSql @ [" "; text; " "] @ rightSql, leftParams @ rightParams, leftCtor @ rightCtor)

            let result : option<string list * PreparedParameter<_> list * ConstructionInfo list>= 
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
                        let any, ml = getMethod "Any" ml
                        
                        let wheres = 
                            match any with 
                            | None -> wheres
                            | Some any -> 
                                wheres @ [any]

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
                            mapd {TableAlias = Some tableAlias; TopQuery = false} e

                        let getReturnType () =
                            let isSome l = l |> List.exists(fun (m : option<_>)-> m.IsSome)
                            if [single; first] |> isSome then
                                Single
                            else if [singleOrDefault; firstOrDefault] |> isSome then
                                SingleOrDefault
                            else
                                Many

                        let createTypeSelect t = 
                            let q, ctor = createTypeSelect getColumnName tableAlias context.TopQuery t
                            q, [], ctor
                        let createFullTypeSelect t =
                            let q, p, ctor = createTypeSelect t
                            let ctor = 
                                match ctor with
                                | None -> []
                                | Some ctor -> 
                                    [{
                                        ReturnType = (getReturnType())
                                        Type = ctor.Type
                                        TypeOrLambda = TypeOrLambdaConstructionInfo.Type ctor
                                    }]
                            q, p, ctor
                        let selectOrDelete, selectParameters, selectCtor =
                            if context.TopQuery && queryType = DeleteQuery then
                                ["DELETE " ] @ tableAlias @ [" "], [], []
                            else
                                let selectColumn, selectParameters, selectCtor = 
                                    match count with
                                    | Some _-> 
                                        ["COUNT(*) "], [], [createConstructionInfoForType 0 typedefof<int> Single]
                                    | None -> 
                                        if contains.IsSome || any.IsSome then
                                            ["CASE WHEN COUNT(*) > 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END "], [] , [createConstructionInfoForType 0 typedefof<bool> Single]
                                        else
                                            let partialSelect (l : LambdaExpression) =
                                                let t = l.ReturnType
                                                let c = 
                                                    if context.TopQuery then
                                                        [createConstructionInfoForType 0 t Many]
                                                    else
                                                        []
                                                let q, p, _ = (l.Body |> map) 
                                                q @ [" "], p, c
                                            match maxOrMin with 
                                            | Some m ->
                                                partialSelect (getLambda m)
                                            | None -> 
                                                match select with
                                                | Some select ->
                                                    match getLambda(select) with
                                                    | SingleSameSelect x -> createFullTypeSelect x.Type
                                                    | l ->
                                                        match l.Body with
                                                        | MemberAccess _ -> partialSelect l
                                                        | Call _m  -> 
                                                            if not context.TopQuery then
                                                                failwith "Calls are only allowed in top select"
                                                            if queryType = DeleteQuery then
                                                                failwith "Calls are not allowed in DeleteQuery"
                                                            let rec isParameter e = 
                                                                match e with
                                                                | Parameter _ -> true
                                                                | MemberAccess m -> isParameter m.Expression
                                                                | _ -> false
                                                                
//                                                            let queryArgs =
//                                                                 m.Arguments 
//                                                                 |> Seq.filter(isParameter)
//
//                                                            printfn "%A" queryArgs

                                                            //let needsSelect = 
//                                                                 |> Seq.map(fun m -> 
//                                                                     let changed 
//                                                                     m, m //changed
//                                                                 )
//                                                                 |> dict

//                                                            let methodArgs = 
//                                                                m.Arguments 
//                                                                |> Seq.map(fun arg -> 
//                                                                    if isParameter arg then
//                                                                        arg
//                                                                    else
//                                                                        arg
//                                                                )
                                                           // let body = Expression.Call(m.Method, methodArgs)
                                                            //let rewrittenExpression = Expression.Lambda(body, methodArgs)
                                                            //printfn "%A" rewrittenExpression
                                                            //take arguments, map to new argument sequence
                                                            // check if the node type is parameter, transform it then
                                                            // select all arguments where node type is parameter
                                                            //failwith "not implemented call"
                                                            let selectQuery, selectParams, typeCtor = 
                                                                createTypeSelect (Queryable.TypeSystem.getElementType (queryable.GetType()))
                                                            let typeCtor = 
                                                                match typeCtor with
                                                                | Some typeCtor -> typeCtor
                                                                | None -> failwith "shouldnt be none"

                                                            let lambdaCtor = {
                                                                Lambda = l
                                                                Parameters = [Type typeCtor]
                                                            }

                                                            let ctor = {
                                                                Type = l.ReturnType
                                                                ReturnType = getReturnType()
                                                                TypeOrLambda = TypeOrLambdaConstructionInfo.Lambda lambdaCtor
                                                            }

                                                            selectQuery, selectParams, [ctor]
                                                        | _ -> failwith "not implemented lambda body"
                                                | None -> 
                                                    createFullTypeSelect (Queryable.TypeSystem.getElementType (queryable.GetType()))
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
                                ["SELECT " ] @ topStatement @ selectColumn, selectParameters, selectCtor

                        let mainStatement = selectOrDelete @ ["FROM "; getTableName(queryable.ElementType); " AS "; ] @ tableAlias

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
                                                    | CallIQueryable(_m, q, rest) ->  
                                                        let containsVal = rest |> Seq.head
                                                        match containsVal with
                                                        | MemberAccess a -> 
                                                            let accessQ, accessP, accessCtor = a |> map
                                                            let subQ, subP, subCtor = q |> map
                                                            Some ([accessQ @ [" IN ("] @ subQ @ [")"]], accessP @ subP, accessCtor @ subCtor)
                                                        | _ -> 
                                                            None
                                                    | _ -> None
                                                | _ -> None
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

                        let sql = mainStatement @ whereClause @ orderByClause
                        let parameters = orderByParameters @ whereParameters @ selectParameters
                        let ctor = orderByCtor @ whereCtor @ selectCtor
                        Some (sql, parameters, ctor)
                    | None ->
                        let simpleInvoke m = 
                            let v = invoke m
                            Some (v |> valueToQueryAndParam (getDBType (TypeSource.Value v)))

                        match m.Method.Name with
                        | "Contains" | "StartsWith" | "EndsWith" as typeName when(m.Object.Type = typedefof<string>) ->
                            let value = (m.Arguments.Item(0)  :?> ConstantExpression).Value
                            let valQ, valP, valC = valueToQueryAndParam (getDBType (TypeSource.Value value)) value
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
                            Some (createNull (getDBType (TypeSource.Type t)))
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
                        failwith "This should ever get hit"
                    else if c.Value = null then
                        Some (["NULL"] ,[], [])
                    else
                        Some (valueToQueryAndParam (getDBType (TypeSource.Value c.Value)) c.Value)
                | MemberAccess m ->
                    if m.Expression <> null && m.Expression.NodeType = ExpressionType.Parameter then
                        match context.TableAlias with
                        | Some tableAlias -> Some (tableAlias @ ["."; getColumnName(m.Member)], [], [])
                        | None -> failwith "cannot access member without tablealias being genned"
                    else
                        match getLocalValue m with
                        | Some (value) -> 
                            let param = valueToQueryAndParam (getDBType (TypeSource.Value value)) value
                            Some (param)
                        | None -> failwithf "The member '%s' is not supported" m.Member.Name
//                        let accessChain = getConstantAccessChain m
//                        printfn "%A" accessChain
                        
//                        match accessChain with
//                        | Some constant, accesses ->
//                            failwith "foobar"
//                        | None -> 
//                            failwithf "The member '%s' is not supported" m.Member.Name
                | _ -> None

            match result with
            | Some r -> ExpressionResult.Skip, r
            | None -> ExpressionResult.Recurse, ([], [], [])

        let results = 
            expression
            |> mapd(mapFun) ({TableAlias = None; TopQuery = true})

        let queryList, queryParameters, resultConstructionInfo = results |> splitResults

        let query = queryList |> String.concat("")

        {
            PreparedStatement.Text = query
            FormattedText = query
            Parameters = queryParameters
            ResultConstructionInfo = 
                if resultConstructionInfo |> Seq.isEmpty then
                    None
                else
                    Some (resultConstructionInfo |> Seq.exactlyOne)
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

    let translateToCommand queryDialect queryType getDBType getTableName getColumnName connection expression =
        let ps = translate queryDialect queryType getDBType getTableName getColumnName expression

        let cmd = createCommand connection ps

        cmd, ps.ResultConstructionInfo