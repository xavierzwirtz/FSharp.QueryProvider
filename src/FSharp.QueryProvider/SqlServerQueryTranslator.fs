namespace FSharp.QueryProvider.QueryTranslator

open FSharp.QueryProvider

type PreparedParameter<'T> = {
    Name : string
    Value : obj 
    DbType : 'T
}

type ConstructionInfo = {
    Type : System.Type
    ConstructorArgs : int seq
    PropertySets : (int * System.Reflection.PropertyInfo) seq
}

type PreparedStatement<'P> = {
    Text : string
    FormattedText : string
    Parameters : PreparedParameter<'P> seq
    ConstructionInfo : ConstructionInfo seq
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

    type private Context = {
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

    type DbTypeOrObject =
    | Object
    | SqlDbType of System.Data.SqlDbType

    let typeToDbType (t : System.Type) =
        match System.Type.GetTypeCode(t) with 
        | System.TypeCode.Boolean -> SqlDbType System.Data.SqlDbType.Bit
        | System.TypeCode.String -> SqlDbType System.Data.SqlDbType.NVarChar
        | System.TypeCode.DateTime -> SqlDbType System.Data.SqlDbType.DateTime2
        | System.TypeCode.Byte -> SqlDbType System.Data.SqlDbType.TinyInt
        | System.TypeCode.Int16 -> SqlDbType System.Data.SqlDbType.SmallInt
        | System.TypeCode.Int32 -> SqlDbType System.Data.SqlDbType.Int
        | System.TypeCode.Int64 -> SqlDbType System.Data.SqlDbType.BigInt
        | System.TypeCode.Object -> 
            Object
        | t -> failwithf "not implemented type '%s'" (t.ToString())

    let createParameter columnIndex value dbType = 
        let p = {
            PreparedParameter.Name = "p" + columnIndex.ToString()
            Value = value
            DbType = dbType
        }

        ["@"; p.Name; ""], [p], List.empty<ConstructionInfo>

    let rec valueToQueryAndParam (columnIndex : int) (value : obj) = 
        let t = value.GetType()

        let dbType = typeToDbType(t)
      
        match dbType with
        | Object ->
            if t |> isOption then 
                t.GetMethod("get_Value").Invoke(value, [||]) |> valueToQueryAndParam columnIndex
            else
                failwithf "The constant for '%A' is not supported" value
        | SqlDbType dbType ->
            createParameter columnIndex value dbType

    let createNull (columnIndex : int) (t : DbTypeOrObject) =
        match t with
        | Object -> failwith "this value cannot be object type"
        | SqlDbType dbType ->
            createParameter columnIndex System.DBNull.Value dbType


//    let createConstructionInfoExplicitFields (fields : System.Reflection.PropertyInfo seq) selectIndex (t : System.Type) =
//        let ctorArgs = fields |> Seq.mapi(fun i f ->
//            selectIndex + i
//        )
//
//        {
//            Type = t
//            ConstructorArgs = ctorArgs
//            PropertySets = []
//        }

    //terible duplication of code between this and createTypeSelect. needs to be refactored.
    let createConstructionInfo selectIndex (t : System.Type) =
        if Microsoft.FSharp.Reflection.FSharpType.IsRecord t then
            let fields = Microsoft.FSharp.Reflection.FSharpType.GetRecordFields t |> Seq.toList

            let ctorArgs = fields |> Seq.mapi(fun i f ->
                selectIndex + i
            )

            {
                Type = t
                ConstructorArgs = ctorArgs
                PropertySets = []
            }, Seq.length(fields) 
        else
            let x = typedefof<int>
            let simple () = 
                {
                    Type = t
                    ConstructorArgs = [selectIndex] 
                    PropertySets = []
                }, 1
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

    let createTypeSelect (tableAlias : string list) (selectIndex : int) (topSelect : bool) (t : System.Type) =
        // need to call a function here so that this can be extended
        if Microsoft.FSharp.Reflection.FSharpType.IsRecord t then
            let fields = Microsoft.FSharp.Reflection.FSharpType.GetRecordFields t |> Seq.toList

            let query = 
                fields 
                |> List.map(fun  f -> tableAlias @ ["."; f.Name]) 
                |> List.interpolate([[", "]])
                |> List.reduce(@)

            let query = query @ [" "]

            let ctor = 
                if topSelect then
                    Some (fst(createConstructionInfo selectIndex t))
                else
                    None

            query, ctor, Seq.length(fields)
        else
            failwith "not implemented, only records are currently implemented"

    let translate (expression : Expression) = 
        
        let columnNameUnique = ref 0

        let getColumnNameIndex () = 
            columnNameUnique := (!columnNameUnique + 1)
            !columnNameUnique

        let selectIndex = ref 0

        let incrementSelectIndex amount = 
            selectIndex := (!selectIndex + amount)

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

            let valueToQueryAndParam value = 
                valueToQueryAndParam (getColumnNameIndex()) value
            let createNull value = 
                createNull (getColumnNameIndex()) value
            let createTypeSelect tableAlias t = 
                let q, ctorOption, columnIndex = createTypeSelect tableAlias (!selectIndex) context.TopSelect t
                incrementSelectIndex columnIndex
                let c = 
                    match ctorOption with
                    | None -> []
                    | Some c -> [c]
                q, [], c

            let createConstructionInfo t = 
                let ctor, i = createConstructionInfo !selectIndex t
                incrementSelectIndex i
                ctor

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
                                ["COUNT(*) "], [], [createConstructionInfo typedefof<int>]
                            | None -> 
                                match contains with 
                                | Some c -> 
                                    ["CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END "], [] , [createConstructionInfo typedefof<bool>]
                                | None -> 
                                    let partialSelect (l : LambdaExpression) =
                                        let t = l.ReturnType
                                        let c = 
                                            if context.TopSelect then
                                                [createConstructionInfo t]
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
//                                            match wheres with 
//                                            | [] -> failwith "cannot determine type" //star
//                                            | _ -> 
//                                                let f = wheres |> Seq.head
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
                            let table = queryable.ElementType.Name
                            ["SELECT " ] @ topStatement @ selectColumn @ ["FROM "; table; " AS "; ] @ tableAlias

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
                        match m.Method.Name with
                        | "Contains" | "StartsWith" | "EndsWith" as typeName when(m.Object.Type = typedefof<string>) ->
                            let arg = m.Arguments.Item(0) 
                            let valQ, valP, valC = valueToQueryAndParam ((arg :?> ConstantExpression).Value)
                            let search =
                                match typeName with 
                                | "Contains" -> ["'%' + "] @ valQ @ [" + '%'"]
                                | "StartsWith" -> valQ @ [" + '%'"]
                                | "EndsWith" -> ["'%' + "] @ valQ
                                | _ -> failwithf "not implemented %s" typeName
                            let colQ, colP, colC = map(m.Object)

                            Some (colQ @ [" LIKE "] @ search, colP @ valP, colC @ valC)
                        | "Invoke" | "op_Dereference" -> 
                            Some (invoke m |> valueToQueryAndParam)
                        | "Some" when (isOption m.Method.ReturnType) -> 
                            Some (invoke m |> valueToQueryAndParam)
                        | "get_None" when (isOption m.Method.ReturnType) -> 
                            let t = m.Method.ReturnType.GetGenericArguments() |> Seq.head
                            Some (createNull(t |> typeToDbType))
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
                        Some (valueToQueryAndParam c.Value)
                | MemberAccess m ->
//                    failwith "should be handled explicitly"
                    if m.Expression <> null && m.Expression.NodeType = ExpressionType.Parameter then
                        //this needs to call a function to determine column name for extensibility.    
                        match context.TableAlias with
                        | Some tableAlias -> Some (tableAlias @ ["."; m.Member.Name], [], [])
                        | None -> failwith "cannot access member without tablealias being genned"
                    else
                        failwithf "The member '%s' is not supported" m.Member.Name
                | _ -> None

            match result with
            | Some r -> ExpressionResult.Skip, r
            | None -> ExpressionResult.Recurse, ([], [], [])
//            if result.IsSome then 
//                ExpressionResult.Skip, result
//            else
//                ExpressionResult.Recurse, result

        let results = 
            expression
            |> mapd(mapFun) ({TableAlias = None; TopSelect = true})

        let queryList, queryParameters, constructionInfo =  results |> splitResults

        let query = queryList |> String.concat("")

        {
            PreparedStatement.Text = query
            FormattedText = query
            Parameters = queryParameters //Seq.empty<PreparedParameter<System.Data.SqlDbType>>
            ConstructionInfo = constructionInfo
        }