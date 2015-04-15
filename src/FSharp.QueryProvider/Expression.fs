module FSharp.QueryProvider.Expression

open System.Linq.Expressions

type ExpressionResult = 
     | Recurse
     | Replace of Expression
     | Skip

type Visitor(visit : Expression -> ExpressionResult) =
    inherit ExpressionVisitor()

    override this.Visit expression =
        let result = visit expression
        match result with
        | Recurse -> base.Visit expression
        | Replace e -> e
        | Skip -> expression
        
let rec stripQuotes (expression : Expression) = 
    if expression.NodeType = ExpressionType.Quote then
        stripQuotes ((expression :?> UnaryExpression).Operand)
    else 
        expression

let rec visit transform (expression : Expression)  =
    
    let visitor = Visitor(transform)
    visitor.Visit expression

let rec map (mapping : Expression -> ExpressionResult * option<list<'t>>) (expression : Expression) : 't list =
    
    let totalResult = ref List.empty<'t>
    let visitor = 
        Visitor(fun e ->
            let expResult,result = mapping e

            if result.IsSome then
                totalResult := (!totalResult |> List.append result.Value)
            expResult
        )

    visitor.Visit expression |> ignore

    !totalResult