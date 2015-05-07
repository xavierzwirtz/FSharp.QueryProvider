[<AutoOpen>]
module Utils

open System.Linq.Expressions
let toLambda quote = 
    let raw = Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.QuotationToExpression quote
    let methodCall = raw :?> MethodCallExpression
    methodCall.Arguments.Item(0) :?> LambdaExpression