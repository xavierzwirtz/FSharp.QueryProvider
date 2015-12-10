[<AutoOpen>]
module Utils

open System.Linq.Expressions

type IQueryable = System.Linq.IQueryable
type IGrouping<'T1, 'T2> = System.Linq.IGrouping<'T1, 'T2>
type IQueryable<'T> = System.Linq.IQueryable<'T>
type Expression = System.Linq.Expressions.Expression

let toLambda quote = 
    let raw = Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.QuotationToExpression quote
    let methodCall = raw :?> MethodCallExpression
    methodCall.Arguments.Item(0) :?> LambdaExpression

type Assert = Xunit.Assert
type Fact = Xunit.FactAttribute