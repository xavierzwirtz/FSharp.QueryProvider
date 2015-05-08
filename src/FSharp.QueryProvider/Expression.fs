module FSharp.QueryProvider.Expression

open System.Linq.Expressions

type ExpressionResult = 
     | Recurse
     | Replace of Expression
     | Skip

type private Visitor(visit : Expression -> ExpressionResult) =
    inherit ExpressionVisitor()

    override this.Visit expression =
        let result = visit expression
        match result with
        | Recurse -> base.Visit expression
        | Replace e -> e
        | Skip -> expression
        
let rec internal stripQuotes (expression : Expression) = 
    if expression.NodeType = ExpressionType.Quote then
        stripQuotes ((expression :?> UnaryExpression).Operand)
    else 
        expression

/// <summary>
/// Walks an expression tree, replacing nodes.
/// </summary>
/// <param name="transform"></param>
/// <param name="expression"></param>
let rec visit transform (expression : Expression)  =
    
    let visitor = Visitor(transform)
    visitor.Visit expression

/// <summary>
/// Walks an expression tree and generates a 't list
/// </summary>
/// <param name="mapping"></param>
/// <param name="expression"></param>
let rec map (mapping : Expression -> ExpressionResult * 't) (expression : Expression) : 't list =
    
    let totalResult = ref List.empty<'t>
    let visitor = 
        Visitor(fun e ->
            let expResult, result = mapping e

            totalResult := (!totalResult |> List.append [result])
            expResult
        )

    visitor.Visit expression |> ignore

    !totalResult

/// <summary>
/// Walks an expression tree and generates a 't list while also passing 'd data down
/// </summary>
/// <param name="mapping"></param>
/// <param name="expression"></param>
let rec mapd (mapping : 'd -> Expression -> ExpressionResult * 't) (data : 'd) (expression : Expression) : 't list =
    map (mapping data) expression