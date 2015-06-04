module FSharp.QueryProvider.QueryOperations
open FSharp.QueryProvider.PreparedQuery

type Sql = 
| S of string
| NP of string
| P of obj

type Parameter = {
    Name : string
    Value : obj
}
type ISqlQuery = 
    abstract Query : Sql seq
    abstract Parameters : Parameter seq

type SqlQuery<'T>(provider, expression, query : Sql seq, parameters : Parameter seq) = 
        inherit Queryable.Query<'T>(provider, expression)
        interface ISqlQuery with
            member this.Query = query
            member this.Parameters = parameters

let directSql provider query parameters : System.Linq.IQueryable<_> = 
    SqlQuery (provider, None, query, parameters) :> System.Linq.IQueryable<_>