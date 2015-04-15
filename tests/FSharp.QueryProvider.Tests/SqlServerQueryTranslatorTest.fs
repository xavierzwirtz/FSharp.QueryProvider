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
            let r = f()
            match r with
            | :? System.Linq.IQueryable<_> as q-> Some (q :> System.Linq.IQueryable)
            | :? System.Linq.IQueryable as q-> Some q
            | _ -> None
            //Some (r :?> System.Linq.IQueryable<_>)
        with 
        | e when(e.Message = "EmptyQueryProvider.Execute not implemented") -> None

    if query.IsSome then
        query.Value.Expression
    else
        let afterCount = provider.Expressions |> Seq.length 
        if afterCount > (beforeCount + 1) then
            let expressionMessage = 
                provider.Expressions 
                    |> Seq.skip(beforeCount)
                    |> Seq.map(fun e -> e.ToString())
                    |> String.concat "\n"
            failwithf "too many expressions fired:\n%s" expressionMessage
        else if afterCount < (beforeCount + 1) then
            failwith "no queriesFired"
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

    [<Test>]
    let ``lastOrDefault``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                lastOrDefault
            } :> obj
        
        let e = getExpression q
        let exc = Assert.Throws(fun () -> 
            QueryTranslator.SqlServer.translate e |> ignore)
        Assert.AreEqual(exc.Message, "'lastOrDefault' operator has no translations for Sql Server")

    [<Test>]
    let ``exactlyOne``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                exactlyOne
            } :> obj
        
        AreEqualExpression q "SELECT TOP 2 * FROM Person"

    [<Test>]
    let ``exactlyOne where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                exactlyOne
            } :> obj
        
        AreEqualExpression q "SELECT TOP 2 * FROM Person WHERE (PersonName = 'john')"

    [<Test>]
    let ``exactlyOneOrDefault``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                exactlyOneOrDefault
            } :> obj
        
        AreEqualExpression q "SELECT TOP 2 * FROM Person"

    [<Test>]
    let ``exactlyOneOrDefault where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                exactlyOneOrDefault
            } :> obj
        
        AreEqualExpression q "SELECT TOP 2 * FROM Person WHERE (PersonName = 'john')"

    [<Test>]
    let ``head``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                head
            } :> obj
        
        AreEqualExpression q "SELECT TOP 1 * FROM Person"

    [<Test>]
    let ``head where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                head
            } :> obj
        
        AreEqualExpression q "SELECT TOP 1 * FROM Person WHERE (PersonName = 'john')"

    [<Test>]
    let ``headOrDefault``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                headOrDefault
            } :> obj
        
        AreEqualExpression q "SELECT TOP 1 * FROM Person"

    [<Test>]
    let ``headOrDefault where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                head
            } :> obj
        
        AreEqualExpression q "SELECT TOP 1 * FROM Person WHERE (PersonName = 'john')"

    [<Test>]
    let ``minBy``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                minBy p.PersonId
            } :> obj
        
        AreEqualExpression q "SELECT TOP 1 PersonID FROM Person ORDER BY PersonID ASC"
    
    [<Test>]
    let ``minBy where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                minBy p.PersonId
            } :> obj
        
        AreEqualExpression q "SELECT TOP 1 PersonID FROM Person WHERE (PersonName = 'john') ORDER BY PersonID ASC"

    [<Test>]
    let ``maxBy``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                maxBy p.PersonId
            } :> obj
        
        AreEqualExpression q "SELECT TOP 1 PersonID FROM Person ORDER BY PersonID DESC"
    
    [<Test>]
    let ``maxBy where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                maxBy p.PersonId
            } :> obj
        
        AreEqualExpression q "SELECT TOP 1 PersonID FROM Person WHERE (PersonName = 'john') ORDER BY PersonID DESC"

    [<Test>]
    [<Ignore>]
    let ``groupBy select count``() =
        //This is broken in the fsharp compiler.
        //https://github.com/Microsoft/visualfsharp/issues/47

        let q = fun () -> 
            query {
                for p in queryable<Person> do
                groupBy p.JobKind into g
                select (g, query { for x in g do count })
            } :> obj

        AreEqualExpression q "SELECT PersonID, COUNT(*) FROM Person GROUP BY PersonID"

    [<Test>]
    let ``groupBy select``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                groupBy p.JobKind into g
                select g
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person"
        //the group by must be done clientside

    [<Test>]
    let ``groupBy where select``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                where(p.PersonName = "john")
                groupBy p.JobKind into g
                select g
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person WHERE (PersonName = 'john')"
        //the group by must be done clientside

    [<Test>]
    let ``sortBy``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                sortBy p.PersonId
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person ORDER BY PersonID ASC"

    [<Test>]
    let ``sortBy thenBy``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                sortBy p.PersonId
                thenBy p.PersonName
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person ORDER BY PersonID ASC, PersonName ASC"

    [<Test>]
    let ``sortBy thenByDescending``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                sortBy p.PersonId
                thenByDescending p.PersonName
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person ORDER BY PersonID ASC, PersonName DESC"

    [<Test>]
    let ``sortBy where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                sortBy p.PersonId
                where(p.PersonName = "john")
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person WHERE (PersonName = 'john') ORDER BY PersonID ASC"

    [<Test>]
    let ``sortByDescending``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                sortByDescending p.PersonId
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person ORDER BY PersonID DESC"

    [<Test>]
    let ``sortByDescending thenBy``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                sortByDescending p.PersonId
                thenBy p.PersonName
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person ORDER BY PersonID DESC, PersonName ASC"

    [<Test>]
    let ``sortByDescending thenByDescending``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                sortByDescending p.PersonId
                thenByDescending p.PersonName
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person ORDER BY PersonID DESC, PersonName DESC"

    [<Test>]
    let ``sortByDescending where``() =
        let q = fun () -> 
            query {
                for p in queryable<Person> do
                sortByDescending p.PersonId
                where(p.PersonName = "john")
            } :> obj
            
        AreEqualExpression q "SELECT * FROM Person WHERE (PersonName = 'john') ORDER BY PersonID DESC"

// To be implemented:        
//query {
//    for student in db.Student do
//    sumByNullable student.Age
//    }
//|> (fun sum -> printfn "Sum of ages: %s" (sum.Print()))
//
//query {
//    for student in db.Student do
//    minByNullable student.Age
//    }
//|> (fun age -> printfn "Minimum age: %s" (age.Print()))
//
//query {
//    for student in db.Student do
//    maxByNullable student.Age
//    }
//|> (fun age -> printfn "Maximum age: %s" (age.Print()))
//
//query {
//    for student in db.Student do
//    averageBy (float student.StudentID)
//    }
//|> printfn "Average student ID: %f"
// 
//query {
//    for student in db.Student do
//    averageByNullable (Nullable.float student.Age)
//    }
//|> (fun avg -> printfn "Average age: %s" (avg.Print()))
//
//query {
//    for student in db.Student do
//    find (student.Name = "Abercrombie, Kim")
//}
//|> (fun student -> printfn "Found a match with StudentID = %d" student.StudentID)
//
//query {
//    for student in db.Student do
//    all (SqlMethods.Like(student.Name, "%,%"))
//}
//|> printfn "Do all students have a comma in the name? %b"
//
//query {
//    for student in db.Student do
//    head
//    }
//|> (fun student -> printfn "Found the head student with StudentID = %d" student.StudentID)
//
//query {
//    for numbers in data do
//    nth 3
//    }
//|> printfn "Third number is %d"
//
//query {
//    for student in db.Student do
//    skip 1
//    }
//|> Seq.iter (fun student -> printfn "StudentID = %d" student.StudentID)
//
//query {
//    for number in data do
//    skipWhile (number < 3)
//    select number
//    }
//|> Seq.iter (fun number -> printfn "Number = %d" number)
//
//query {
//   for student in db.Student do
//   sumBy student.StudentID
//   }
//|> printfn "Sum of student IDs: %d" 
//
//query {
//   for student in db.Student do
//   select student
//   take 2
//   }
//|> Seq.iter (fun student -> printfn "StudentID = %d" student.StudentID)
//
//query {
//    for number in data do
//    takeWhile (number < 10)
//    }
//|> Seq.iter (fun number -> printfn "Number = %d" number)
//
//query {
//    for student in db.Student do
//    sortByNullable student.Age
//    select student
//}
//|> Seq.iter (fun student ->
//    printfn "StudentID, Name, Age: %d %s %s" student.StudentID student.Name (student.Age.Print()))
//
//query {
//    for student in db.Student do
//    sortByNullableDescending student.Age
//    select student
//}
//|> Seq.iter (fun student ->
//    printfn "StudentID, Name, Age: %d %s %s" student.StudentID student.Name (student.Age.Print()))
//
//query {
//    for student in db.Student do
//    sortBy student.Name
//    thenByNullable student.Age
//    select student
//}
//|> Seq.iter (fun student ->
//    printfn "StudentID, Name, Age: %d %s %s" student.StudentID student.Name (student.Age.Print()))
//
//query {
//    for student in db.Student do
//    sortBy student.Name
//    thenByNullableDescending student.Age
//    select student
//}
//|> Seq.iter (fun student ->
//    printfn "StudentID, Name, Age: %d %s %s" student.StudentID student.Name (student.Age.Print()))
//
//query {
//        for student in db.Student do
//        select student
//    }
//    |> Seq.iter (fun student -> printfn "%s %d %s" student.Name student.StudentID (student.Age.Print()))
//
//query {
//        for student in db.Student do        
//        count
//    }
//|>  (fun count -> printfn "Student count: %d" count)
//
//query {
//        for student in db.Student do
//        where (ExtraTopLevelOperators.query 
//                      { for courseSelection in db.CourseSelection do
//                        exists (courseSelection.StudentID = student.StudentID) })
//        select student }
//|> Seq.iter (fun student -> printfn "%A" student.Name)
//
//query {
//        for n in db.Student do
//        groupBy n.Age into g
//        select (g.Key, g.Count())
//}
//|> Seq.iter (fun (age, count) -> printfn "%s %d" (age.Print()) count)
//
//query {
//        for n in db.Student do
//        groupValBy n.Age n.Age into g
//        select (g.Key, g.Count())
//    }
//|> Seq.iter (fun (age, count) -> printfn "%s %d" (age.Print()) count)
//
//query {
//        for student in db.Student do
//        groupBy student.Age into g
//        where (g.Key.HasValue && g.Key.Value > 10)
//        select (g, g.Key)
//}
//|> Seq.iter (fun (students, age) ->
//    printfn "Age: %s" (age.Value.ToString())
//    students
//    |> Seq.iter (fun student -> printfn "%s" student.Name))
//
//query {
//        for student in db.Student do
//        groupBy student.Age into group
//        where (group.Count() > 1)
//        select (group.Key, group.Count())
//}
//|> Seq.iter (fun (age, ageCount) ->
//     printfn "Age: %s Count: %d" (age.Print()) ageCount)
//
//query {
//        for student in db.Student do
//        groupBy student.Age into g        
//        let total = query { for student in g do sumByNullable student.Age }
//        select (g.Key, g.Count(), total)
//}
//|> Seq.iter (fun (age, count, total) ->
//    printfn "Age: %d" (age.GetValueOrDefault())
//    printfn "Count: %d" count
//    printfn "Total years: %s" (total.ToString()))
//
//query {
//        for student in db.Student do
//        groupBy student.Age into g
//        where (g.Count() > 1)        
//        sortByDescending (g.Count())
//        select (g.Key, g.Count())
//}
//|> Seq.iter (fun (age, myCount) ->
//    printfn "Age: %s" (age.Print())
//    printfn "Count: %d" myCount)
//
//
//let idList = [1; 2; 5; 10]
//let idQuery = query { for id in idList do
//                       select id }
//query {
//        for student in db.Student do
//        where (idQuery.Contains(student.StudentID))
//        select student
//        }
//|> Seq.iter (fun student ->
//    printfn "Name: %s" student.Name)
//
//query {
//    for student in db.Student do
//    where (SqlMethods.Like( student.Name, "_e%") )
//    select student
//    take 2   
//    }
//|> Seq.iter (fun student -> printfn "%s" student.Name)
//
//query {
//    for student in db.Student do
//    where (SqlMethods.Like( student.Name, "[abc]%") )
//    select student  
//    }
//|> Seq.iter (fun student -> printfn "%s" student.Name)
//
//query {
//    for student in db.Student do
//    where (SqlMethods.Like( student.Name, "[^abc]%") )
//    select student  
//    }
//|> Seq.iter (fun student -> printfn "%s" student.Name)
//
//query {
//    for n in db.Student do
//    where (SqlMethods.Like( n.Name, "[^abc]%") )
//    select n.StudentID    
//    }
//|> Seq.iter (fun id -> printfn "%d" id)
//
//query {
//        for student in db.Student do
//        where (student.Name.Contains("a"))
//        select student
//    }
//|> Seq.iter (fun student -> printfn "%s" student.Name)
//
//let names = [|"a";"b";"c"|]
//query {
//    for student in db.Student do
//    if names.Contains (student.Name) then select student }
//|> Seq.iter (fun student -> printfn "%s" student.Name)
//
//query {
//        for student in db.Student do 
//        join selection in db.CourseSelection 
//          on (student.StudentID = selection.StudentID)
//        select (student, selection)
//    }
//|> Seq.iter (fun (student, selection) -> printfn "%d %s %d" student.StudentID student.Name selection.CourseID)
//
//query {
//    for student in db.Student do
//    leftOuterJoin selection in db.CourseSelection 
//      on (student.StudentID = selection.StudentID) into result
//    for selection in result.DefaultIfEmpty() do
//    select (student, selection)
//    }
//|> Seq.iter (fun (student, selection) ->
//    let selectionID, studentID, courseID =
//        match selection with
//        | null -> "NULL", "NULL", "NULL"
//        | sel -> (sel.ID.ToString(), sel.StudentID.ToString(), sel.CourseID.ToString())
//    printfn "%d %s %d %s %s %s" student.StudentID student.Name (student.Age.GetValueOrDefault()) selectionID studentID courseID)
//
//query {
//        for n in db.Student do 
//        join e in db.CourseSelection on (n.StudentID = e.StudentID)
//        count        
//    }
//|>  printfn "%d"
//
//query {
//        for student in db.Student do 
//        join selection in db.CourseSelection on (student.StudentID = selection.StudentID)
//        distinct        
//    }
//|> Seq.iter (fun (student, selection) -> printfn "%s %d" student.Name selection.CourseID)
//
//query {
//        for n in db.Student do 
//        join e in db.CourseSelection on (n.StudentID = e.StudentID)
//        distinct
//        count       
//    }
//|> printfn "%d"
//
//query {
//        for student in db.Student do
//        where (student.Age.Value >= 10 && student.Age.Value < 15)
//        select student
//    }
//|> Seq.iter (fun student -> printfn "%s" student.Name)
//
//query {
//        for student in db.Student do
//        where (student.Age.Value = 11 || student.Age.Value = 12)
//        select student
//    }
//|> Seq.iter (fun student -> printfn "%s" student.Name)
//
//query {
//        for n in db.Student do
//        where (n.Age.Value = 12 || n.Age.Value = 13)
//        sortByNullableDescending n.Age
//        select n
//    }
//|> Seq.iter (fun student -> printfn "%s %s" student.Name (student.Age.Print()))
//
//query {
//        for student in db.Student do
//        where ((student.Age.HasValue && student.Age.Value = 11) ||
//               (student.Age.HasValue && student.Age.Value = 12))
//        sortByDescending student.Name 
//        select student.Name
//        take 2
//    }
//|> Seq.iter (fun name -> printfn "%s" name)
//
//module Queries =
//    let query1 = query {
//            for n in db.Student do
//            select (n.Name, n.Age)
//        }
//
//    let query2 = query {
//            for n in db.LastStudent do
//            select (n.Name, n.Age)
//            }
//
//    query2.Union (query1)
//    |> Seq.iter (fun (name, age) -> printfn "%s %s" name (age.Print()))
//
//module Queries2 =
//    let query1 = query {
//           for n in db.Student do
//           select (n.Name, n.Age)
//        }
//
//    let query2 = query {
//            for n in db.LastStudent do
//            select (n.Name, n.Age)
//            }
//
//    query1.Intersect(query2)
//    |> Seq.iter (fun (name, age) -> printfn "%s %s" name (age.Print()))
//
//query {
//        for student in db.Student do
//        select (if student.Age.HasValue && student.Age.Value = -1 then
//                   (student.StudentID, System.Nullable<int>(100), student.Age)
//                else (student.StudentID, student.Age, student.Age))
//    }
//|> Seq.iter (fun (id, value, age) -> printfn "%d %s %s" id (value.Print()) (age.Print()))
//
//query {
//        for student in db.Student do
//        select (if student.Age.HasValue && student.Age.Value = -1 then
//                   (student.StudentID, System.Nullable<int>(100), student.Age)
//                elif student.Age.HasValue && student.Age.Value = 0 then
//                    (student.StudentID, System.Nullable<int>(100), student.Age)
//                else (student.StudentID, student.Age, student.Age))
//    }
//|> Seq.iter (fun (id, value, age) -> printfn "%d %s %s" id (value.Print()) (age.Print()))
//
//query {
//        for student in db.Student do
//        for course in db.Course do
//        select (student, course)
//}
//|> Seq.iteri (fun index (student, course) ->
//    if (index = 0) then printfn "StudentID Name Age CourseID CourseName"
//    printfn "%d %s %s %d %s" student.StudentID student.Name (student.Age.Print()) course.CourseID course.CourseName)
//
//query {
//    for student in db.Student do
//    join courseSelection in db.CourseSelection on
//        (student.StudentID = courseSelection.StudentID)
//    join course in db.Course on
//          (courseSelection.CourseID = course.CourseID)
//    select (student.Name, course.CourseName)
//    }
//    |> Seq.iter (fun (studentName, courseName) -> printfn "%s %s" studentName courseName)
//
//query {
//    for student in db.Student do
//    leftOuterJoin courseSelection in db.CourseSelection 
//      on (student.StudentID = courseSelection.StudentID) into g1
//    for courseSelection in g1.DefaultIfEmpty() do
//    leftOuterJoin course in db.Course 
//      on (courseSelection.CourseID = course.CourseID) into g2
//    for course in g2.DefaultIfEmpty() do
//    select (student.Name, course.CourseName)
//    }
//|> Seq.iter (fun (studentName, courseName) -> printfn "%s %s" studentName courseName)