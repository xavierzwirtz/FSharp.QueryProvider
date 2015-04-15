module SqlServerQueryTranslatorTest

open NUnit.Framework
open FSharp.QueryProvider

open Models

type Expression = System.Linq.Expressions.Expression

let provider = EmptyQueryProvider.EmptyQueryProvider()

let queryable<'T> = Queryable.Query(provider, None)

let getExpression (f : unit -> obj) = 
    let beforeCount = provider.Expressions |> Seq.length 
    let query =
        try
            Some (f() :?> System.Linq.IQueryable<_>)
        with 
        | _ -> None

    if query.IsSome then
        query.Value.Expression
    else
        let afterCount = provider.Expressions |> Seq.length 
        if afterCount <> (beforeCount + 1) then
            failwith "count is wrong"
        else
            provider.Expressions |> Seq.last

let AreEqualExpression get expectedSql : unit =
        
    let expression = getExpression get

    printfn "%s" (expression.ToString())
    let sqlQuery = QueryTranslator.SqlServer.translate expression
    let actualSql = sqlQuery.Text

    Assert.AreEqual(expectedSql, actualSql)
    printfn "%s" (actualSql)

//use for test data:
//http://fsprojects.github.io/FSharp.Linq.ComposableQuery/QueryExamples.html
//https://msdn.microsoft.com/en-us/library/vstudio/hh225374.aspx
module QueryGenTest = 


//    let AreEqualQuery (query : 't System.Linq.IQueryable) (expectedSql : string) : unit =
//        AreEqualExpression (query.Expression) expectedSql

    [<Test>]
    let ``simple select``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                select p
            } :> obj
        
        AreEqualExpression q "SELECT * FROM Person"

    [<Test>]
    let ``simple where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                select p
            } :> obj
        
        AreEqualExpression q "SELECT * FROM Person WHERE (PersonName = 'john')"

    [<Test>]
    let ``where with single or``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john" || p.PersonName = "doe")
                select p
            } :> obj
        
        AreEqualExpression q "SELECT * FROM Person WHERE (PersonName = 'john' OR PersonName = 'doe')"

    [<Test>]
    let ``where with two or``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john" || p.PersonName = "doe" || p.PersonName = "james")
                select p
            } :> obj
        
        AreEqualExpression q "SELECT * FROM Person WHERE (PersonName = 'john' OR PersonName = 'doe' OR PersonName = 'james')"

    [<Test>]
    let ``where string contains``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName.Contains("john"))
                select p
            } :> obj
        
        AreEqualExpression q "SELECT * FROM Person WHERE (PersonName LIKE '%john%')"

    [<Test>]
    let ``partial select``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                select p.PersonName
            } :> obj
        
        AreEqualExpression q "SELECT PersonName FROM Person"

    [<Test>]
    let ``partial select with where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                select p.PersonName
            } :> obj
        
        AreEqualExpression q "SELECT PersonName FROM Person WHERE (PersonName = 'john')"

    [<Test>]
    [<Ignore>]
    let ``partial tuple select``() =
        //This is broken in the fsharp compiler.
        //https://github.com/Microsoft/visualfsharp/issues/47

        let q = fun () -> 
            query {
                for p in queryable<Person> do
                select (p.PersonName, p.PersonId)
            } :> obj
        
        AreEqualExpression q "SELECT T0.PersonName, T0.PersonID FROM Person AS T0"

    [<Test>]
    let ``count``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                count
            } :> obj

        AreEqualExpression q "SELECT COUNT(*) FROM Person"

    [<Test>]
    let ``count where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                count
            } :> obj

        AreEqualExpression q "SELECT COUNT(*) FROM Person WHERE (PersonName = 'john')"


    [<Test>]
    let ``contains col``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                select p.PersonId
                contains 11
            } :> obj
        
        AreEqualExpression q "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM Person WHERE (PersonID = '11')"

    [<Test>]
    let ``contains whole``() =
        let e = {
            PersonName = "john"
            JobKind = JobKind.Salesman
            VersionNo = 5
            PersonId = 10
        }

        let q = fun () -> 
            query {
                for p in queryable<Person> do
                contains e
            } :> obj

        let sql = 
            ["SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM Person WHERE ("]
            @ ["PersonName = 'john' AND "]
            @ ["JobKind = '0' AND "]
            @ ["VersionNo = '5' AND "]
            @ ["PersonId = '10')"]

        AreEqualExpression q (sql |> String.concat(""))

    [<Test>]
    let ``contains where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                select p.PersonId
                contains 11
            } :> obj
        
        AreEqualExpression q "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM Person WHERE (PersonName = 'john' AND PersonID = '11')"

    [<Test>]
    let ``last throws``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                last
            } :> obj
        
        let e = getExpression q
        let exc = Assert.Throws(fun () -> 
            QueryTranslator.SqlServer.translate e |> ignore)
        Assert.AreEqual(exc.Message, "'last' operator has no translations for Sql Server")