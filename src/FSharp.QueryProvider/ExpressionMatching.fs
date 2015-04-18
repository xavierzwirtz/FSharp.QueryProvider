module FSharp.QueryProvider.ExpressionMatching

open System.Linq.Expressions

type private et = ExpressionType

let private compare (expression : Expression) nodeType =
    if expression = null then
        None
    else if expression.NodeType = nodeType then
        Some expression
    else
        None

let private cast<'t when 't :> Expression> (e : Expression option) =
    if e.IsSome then
         Some (e.Value :?> 't)
    else
        None

let (|Add|_|) (e : Expression) =
    compare e et.Add
let (|AddAssign|_|) (e : Expression) =
    compare e et.AddAssign
let (|AddAssignChecked|_|) (e : Expression) =
    compare e et.AddAssignChecked
let (|AddChecked|_|) (e : Expression) =
    compare e et.AddChecked
let (|And|_|) (e : Expression) =
    compare e et.And |> cast<BinaryExpression>
let (|AndAlso|_|) (e : Expression) =
    compare e et.AndAlso |> cast<BinaryExpression>
let (|AndAssign|_|) (e : Expression) =
    compare e et.AndAssign
let (|ArrayIndex|_|) (e : Expression) =
    compare e et.ArrayIndex
let (|ArrayLength|_|) (e : Expression) =
    compare e et.ArrayLength
let (|Assign|_|) (e : Expression) =
    compare e et.Assign
let (|Block|_|) (e : Expression) =
    compare e et.Block
let (|Call|_|) (e : Expression) =
    compare e et.Call |> cast<MethodCallExpression>
let (|CallIQueryable|_|) (e : Expression) =
    match compare e et.Call |> cast<MethodCallExpression> with
    | Some m -> 
        if m.Arguments.Count > 0 then
            let first = m.Arguments |> Seq.head 
            if typedefof<System.Linq.IQueryable>.IsAssignableFrom first.Type then
                let rest = m.Arguments |> Seq.skip(1)
                Some (m, first, rest)
            else
                None
        else 
            None
    | None -> None

let (|Coalesce|_|) (e : Expression) =
    compare e et.Coalesce
let (|Conditional|_|) (e : Expression) =
    compare e et.Conditional
let (|Constant|_|) (e : Expression) =
    compare e et.Constant |> cast<ConstantExpression>
let (|Convert|_|) (e : Expression) =
    compare e et.Convert
let (|ConvertChecked|_|) (e : Expression) =
    compare e et.ConvertChecked
let (|DebugInfo|_|) (e : Expression) =
    compare e et.DebugInfo
let (|Decrement|_|) (e : Expression) =
    compare e et.Decrement
let (|Default|_|) (e : Expression) =
    compare e et.Default
let (|Divide|_|) (e : Expression) =
    compare e et.Divide
let (|DivideAssign|_|) (e : Expression) =
    compare e et.DivideAssign
let (|Dynamic|_|) (e : Expression) =
    compare e et.Dynamic
let (|Equal|_|) (e : Expression) =
    compare e et.Equal |> cast<BinaryExpression>
let (|ExclusiveOr|_|) (e : Expression) =
    compare e et.ExclusiveOr |> cast<BinaryExpression>
let (|ExclusiveOrAssign|_|) (e : Expression) =
    compare e et.ExclusiveOrAssign
let (|Extension|_|) (e : Expression) =
    compare e et.Extension
let (|Goto|_|) (e : Expression) =
    compare e et.Goto
let (|GreaterThan|_|) (e : Expression) =
    compare e et.GreaterThan |> cast<BinaryExpression>
let (|GreaterThanOrEqual|_|) (e : Expression) =
    compare e et.GreaterThanOrEqual |> cast<BinaryExpression>
let (|Increment|_|) (e : Expression) =
    compare e et.Increment
let (|Index|_|) (e : Expression) =
    compare e et.Index
let (|Invoke|_|) (e : Expression) =
    compare e et.Invoke
let (|IsFalse|_|) (e : Expression) =
    compare e et.IsFalse
let (|IsTrue|_|) (e : Expression) =
    compare e et.IsTrue
let (|Label|_|) (e : Expression) =
    compare e et.Label
let (|Lambda|_|) (e : Expression) =
    compare e et.Lambda
let (|LeftShift|_|) (e : Expression) =
    compare e et.LeftShift
let (|LeftShiftAssign|_|) (e : Expression) =
    compare e et.LeftShiftAssign
let (|LessThan|_|) (e : Expression) =
    compare e et.LessThan |> cast<BinaryExpression>
let (|LessThanOrEqual|_|) (e : Expression) =
    compare e et.LessThanOrEqual |> cast<BinaryExpression>
let (|ListInit|_|) (e : Expression) =
    compare e et.ListInit
let (|Loop|_|) (e : Expression) =
    compare e et.Loop
let (|MemberAccess|_|) (e : Expression) =
    compare e et.MemberAccess |> cast<MemberExpression>
let (|MemberInit|_|) (e : Expression) =
    compare e et.MemberInit
let (|Modulo|_|) (e : Expression) =
    compare e et.Modulo
let (|ModuloAssign|_|) (e : Expression) =
    compare e et.ModuloAssign
let (|Multiply|_|) (e : Expression) =
    compare e et.Multiply
let (|MultiplyAssign|_|) (e : Expression) =
    compare e et.MultiplyAssign
let (|MultiplyAssignChecked|_|) (e : Expression) =
    compare e et.MultiplyAssignChecked
let (|MultiplyChecked|_|) (e : Expression) =
    compare e et.MultiplyChecked
let (|Negate|_|) (e : Expression) =
    compare e et.Negate
let (|NegateChecked|_|) (e : Expression) =
    compare e et.NegateChecked
let (|New|_|) (e : Expression) =
    compare e et.New
let (|NewArrayBounds|_|) (e : Expression) =
    compare e et.NewArrayBounds
let (|NewArrayInit|_|) (e : Expression) =
    compare e et.NewArrayInit
let (|Not|_|) (e : Expression) =
    compare e et.Not |> cast<UnaryExpression>
let (|NotEqual|_|) (e : Expression) =
    compare e et.NotEqual |> cast<BinaryExpression>
let (|OnesComplement|_|) (e : Expression) =
    compare e et.OnesComplement
let (|Or|_|) (e : Expression) =
    compare e et.Or |> cast<BinaryExpression>
let (|OrAssign|_|) (e : Expression) =
    compare e et.OrAssign
let (|OrElse|_|) (e : Expression) =
    compare e et.OrElse |> cast<BinaryExpression>
let (|Parameter|_|) (e : Expression) =
    compare e et.Parameter
let (|PostDecrementAssign|_|) (e : Expression) =
    compare e et.PostDecrementAssign
let (|PostIncrementAssign|_|) (e : Expression) =
    compare e et.PostIncrementAssign
let (|Power|_|) (e : Expression) =
    compare e et.Power
let (|PowerAssign|_|) (e : Expression) =
    compare e et.PowerAssign
let (|PreDecrementAssign|_|) (e : Expression) =
    compare e et.PreDecrementAssign
let (|PreIncrementAssign|_|) (e : Expression) =
    compare e et.PreIncrementAssign
let (|Quote|_|) (e : Expression) =
    compare e et.Quote
let (|RightShift|_|) (e : Expression) =
    compare e et.RightShift
let (|RightShiftAssign|_|) (e : Expression) =
    compare e et.RightShiftAssign
let (|RuntimeVariables|_|) (e : Expression) =
    compare e et.RuntimeVariables
let (|Subtract|_|) (e : Expression) =
    compare e et.Subtract
let (|SubtractAssign|_|) (e : Expression) =
    compare e et.SubtractAssign
let (|SubtractAssignChecked|_|) (e : Expression) =
    compare e et.SubtractAssignChecked
let (|SubtractChecked|_|) (e : Expression) =
    compare e et.SubtractChecked
let (|Switch|_|) (e : Expression) =
    compare e et.Switch
let (|Throw|_|) (e : Expression) =
    compare e et.Throw
let (|Try|_|) (e : Expression) =
    compare e et.Try
let (|TypeAs|_|) (e : Expression) =
    compare e et.TypeAs
let (|TypeEqual|_|) (e : Expression) =
    compare e et.TypeEqual
let (|TypeIs|_|) (e : Expression) =
    compare e et.TypeIs
let (|UnaryPlus|_|) (e : Expression) =
    compare e et.UnaryPlus
let (|Unbox|_|) (e : Expression) =
    compare e et.Unbox