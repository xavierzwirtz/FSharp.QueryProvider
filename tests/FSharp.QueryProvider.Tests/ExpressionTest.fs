module Expression

open System.Linq.Expressions

open FSharp.QueryProvider.ExpressionMatching
open FSharp.QueryProvider.Expression
open NUnit.Framework

[<Test>]
let ``visit`` () =
    let source = Expression.Add(Expression.Constant(2), Expression.Constant(2))
    
    let actual = 
        source
        |> visit (fun e -> 
            match e with
            | Constant _ -> Replace (Expression.Variable(typedefof<int>, "foo") :> Expression)
            | _ -> Recurse
        )
    
    let expected = Expression.Add(Expression.Variable(typedefof<int>, "foo"), Expression.Variable(typedefof<int>, "foo"))
    
    Assert.AreEqual(expected.ToString(), actual.ToString())

[<Test>]
let ``map``() =
    let source = Expression.Add(Expression.Constant(2), Expression.Constant(2))
    ignore()

    let result = 
        map(fun e -> Recurse,[e.NodeType.ToString()] |> Some) source
        |> String.concat(",")
    Assert.AreEqual("Constant,Constant,Add", result)
    //let