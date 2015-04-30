module FSharp.QueryProvider.Queryable

open FSharp.QueryProvider

open System.Reflection
open System.Linq
open System.Linq.Expressions
open System.Collections
open System.Collections.Generic

module TypeSystem = 
   
    let implements implementerType (interfaceType : System.Type) =
        interfaceType.IsAssignableFrom(implementerType)

    let rec findIEnumerable seqType = 
        if seqType = null || seqType = typedefof<string> then
            None
        else
            if seqType.IsArray then
                Some (typedefof<IEnumerable<_>>.MakeGenericType(seqType.GetElementType()))
            else 
                let assigneable =
                    match seqType.IsGenericType with
                    | true ->
                        seqType.GetGenericArguments()
                        |> Seq.tryFind(fun (arg) -> 
                            typedefof<IEnumerable<_>>.MakeGenericType(arg).IsAssignableFrom(seqType))
                    | false -> None
                if assigneable.IsSome then
                    Some assigneable.Value
                else
                    let iface =
                        let ifaces = seqType.GetInterfaces()
                        if (ifaces <> null) && ifaces.Length > 0 then
                            ifaces |> Seq.tryPick(findIEnumerable)
                        else 
                            None
                    if iface.IsSome then
                        iface
                    else
                        if seqType.BaseType <> null && seqType.BaseType <> typedefof<obj> then
                            findIEnumerable(seqType.BaseType)
                        else 
                            None

    let getElementType seqType =
        let ienum = findIEnumerable(seqType)
        match ienum with 
        | None -> seqType
        | Some(ienum) ->  ienum
            //ienum.GetGenericArguments() |> Seq.head

type Query<'T>(provider : QueryProvider, expression : Expression option) as this = 
    
    let hardExpression = 
        match expression with 
        | None -> Expression.Constant(this) :> Expression
        | Some x -> x

    member __.provider = provider
    member __.expression = hardExpression

    interface IQueryable<'T> with
        member __.Expression =
            hardExpression
        
        member __.ElementType =
            typedefof<'T>
    
        member this.Provider = 
            this.provider :> IQueryProvider

    interface IOrderedQueryable<'T>

    interface IEnumerable with
        member this.GetEnumerator() =
            let x = this.provider.Execute(hardExpression) :?> IEnumerable
            x.GetEnumerator()
    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            let x = this.provider.Execute(hardExpression) :?> IEnumerable<'T>
            x.GetEnumerator()

    override __.ToString () =
        match expression with 
        | Some e -> e.ToString()
        | None -> sprintf "value(Query<%s>)" typedefof<'T>.Name
        //hardExpression.ToString()

and [<AbstractClass>] QueryProvider() =
    interface IQueryProvider with 
        
        member this.CreateQuery<'S> (expression : Expression) = 
            let q = Query<'S>(this, Some expression) :> IQueryable<'S>
            q
        member this.CreateQuery (expression : Expression) = 
            let elementType = TypeSystem.getElementType(expression.Type)
            try
                let generic = typedefof<Query<_>>.MakeGenericType(elementType)

                let args : obj array = [|this; Some expression|]
                let instance = System.Activator.CreateInstance(generic, args)
                instance :?> IQueryable
            with 
            | :? TargetInvocationException as ex -> raise ex.InnerException
        
        member this.Execute<'S> expression =
            this.Execute expression :?> 'S

        member this.Execute (expression) =
            this.Execute expression

    abstract member Execute : Expression -> obj

type DBQueryProvider<'T when 'T :> System.Data.IDbConnection>
    (
    getConnection : unit -> 'T, 
    translate : 'T -> Expression -> System.Data.IDbCommand * DataReader.TypeConstructionInfo,
    onExecutingCommand : option<System.Data.IDbCommand -> System.Data.IDbCommand * obj>,
    onExecutedCommand : option<System.Data.IDbCommand * obj -> unit>
    ) =
    inherit QueryProvider()
    
    override __.Execute expression =
        use connection = getConnection()
        let cmd, ctorInfo = translate connection expression

        let cmd, userState = 
            match onExecutingCommand with
            | None -> cmd, null
            | Some x -> x cmd

        connection.Open()
        use reader = cmd.ExecuteReader()
        let res = DataReader.read reader ctorInfo
        if onExecutedCommand.IsSome then onExecutedCommand.Value(cmd, userState)
        res