module DataReaderTest

open FSharp.QueryProvider
open FSharp.QueryProvider.DataReader

open Models
open LocalDataReader

type UnionEnum =
| One = 0
| Two = 1
| Three = 2

type VerboseRecord = {
    String : string
    Bool : bool
    Byte : byte
    Char : char
    DateTime : System.DateTime
    Decimal : decimal
    Double : double
    Float : float
    Guid : System.Guid
    Int16 : int16
    Int32 : int32
    Int64 : int64
    UnionEnum : UnionEnum
}

type Name = {
    Value : string
}

type OptionName = {
    OptionValue : string option
}

type KeyValuePair = {
    Key : string
    KeyValue : string
}

let ctorOneSimple t = 
    {
        ReturnType = Single
        Type = t
        TypeOrLambda = TypeOrLambdaConstructionInfo.Type {
            Type = t
            ConstructorArgs = [Value 0]
            PropertySets = []
        }
        PostProcess = None
    }

let ctorManySimple t = 
    {
        ReturnType = Many
        Type = t
        TypeOrLambda = TypeOrLambdaConstructionInfo.Type {
            Type = t
            ConstructorArgs = [Value 0]
            PropertySets = []
        }
        PostProcess = None
    }

let ctorName returnType =
    {
        ReturnType = returnType
        Type = typedefof<Name>
        TypeOrLambda = TypeOrLambdaConstructionInfo.Type {
            Type = typedefof<Name>
            ConstructorArgs = [Value 0]
            PropertySets = []
        }
        PostProcess = None
    }

let ctorOptionName =
    createTypeConstructionInfo typedefof<OptionName> [
        Type (createTypeConstructionInfo (Some("").GetType()) [Value 0] [])
    ] []

let ctorOptionNameFull returnType =
    {
        ReturnType = returnType
        Type = typedefof<OptionName>
        TypeOrLambda = TypeOrLambdaConstructionInfo.Type ctorOptionName
        PostProcess = None
    }

let ctorKeyValuePair =
    createTypeConstructionInfo typedefof<OptionName> [
        Value 0; Value 1
    ] []

let ctorVerboseRecord returnType = 
    {
        ReturnType = returnType
        Type = typedefof<VerboseRecord>
        TypeOrLambda = TypeOrLambdaConstructionInfo.Type {
            Type = typedefof<VerboseRecord>
            ConstructorArgs = [0..12] |> Seq.map(fun i -> Value i)
            PropertySets = []
        }
        PostProcess = None
    }

[<Fact>]
let ``one string``() =
    let reader = new LocalDataReader([["foo bar"]])
    let result = (read reader (ctorOneSimple typedefof<string>)) :?> string
    Assert.Equal("foo bar", result)
[<Fact>]
let ``one bool true``() =
    let reader = new LocalDataReader([[1]])
    let result = (read reader (ctorOneSimple typedefof<bool>)) :?> bool
    Assert.Equal(true, result)
[<Fact>]
let ``one bool false``() =
    let reader = new LocalDataReader([[0]])
    let result = (read reader (ctorOneSimple typedefof<bool>)) :?> bool
    Assert.Equal(false , result)
//[<Fact>]
//let ``one byte``() =
//    let reader = new LocalDataReader([[5y]])
//    let result = (read reader (ctorOneSimple typedefof<sbyte>)) :?> sbyte
//    Assert.Equal(5y, result)
[<Fact>]
let ``one ubyte``() =
    let reader = new LocalDataReader([[5uy]])
    let result = (read reader (ctorOneSimple typedefof<byte>)) :?> byte
    Assert.Equal(5uy, result)
[<Fact>]
let ``one char``() =
    let reader = new LocalDataReader([['g']])
    let result = (read reader (ctorOneSimple typedefof<char>)) :?> char
    Assert.Equal('g' , result)
[<Fact>]
let ``one System.DateTime``() =
    let d = System.DateTime.Now
    let reader = new LocalDataReader([[d]])
    let result = (read reader (ctorOneSimple typedefof<System.DateTime>)) :?> System.DateTime
    Assert.Equal(d, result)
[<Fact>]
let ``one decimal``() =
    let reader = new LocalDataReader([[1.23m]])
    let result = (read reader (ctorOneSimple typedefof<decimal>)) :?> decimal
    Assert.Equal(1.23m, result)
[<Fact>]
let ``one double``() =
    let reader = new LocalDataReader([[4.14]])
    let result = (read reader (ctorOneSimple typedefof<double>)) :?> double
    Assert.Equal(4.14, result)
[<Fact>]
let ``one float``() =
    let reader = new LocalDataReader([[4.14]])
    let result = (read reader (ctorOneSimple typedefof<float>)) :?> float
    Assert.Equal(4.14, result)
[<Fact>]
let ``one System.Guid``() =
    let g = System.Guid.NewGuid()
    let reader = new LocalDataReader([[g]])
    let result = (read reader (ctorOneSimple typedefof<System.Guid>)) :?> System.Guid
    Assert.Equal(g, result)
[<Fact>]
let ``one int16``() =
    let reader = new LocalDataReader([[86s]])
    let result = (read reader (ctorOneSimple typedefof<int16>)) :?> int16
    Assert.Equal(86s, result)
[<Fact>]
let ``one int32``() =
    let reader = new LocalDataReader([[5]])
    let result = (read reader (ctorOneSimple typedefof<int32>)) :?> int32
    Assert.Equal(5, result)
[<Fact>]
let ``one int64``() =
    let reader = new LocalDataReader([[5L]])
    let result = (read reader (ctorOneSimple typedefof<int64>)) :?> int64
    Assert.Equal(5L, result)
[<Fact>]
let ``one UnionEnum``() =
    let reader = new LocalDataReader([[1]])
    let result = (read reader (ctorOneSimple typedefof<UnionEnum>)) :?> UnionEnum
    Assert.Equal(UnionEnum.Two, result)

//no need to test the many for every type
[<Fact>]
let ``many string``() =
    let reader = new LocalDataReader([["foo"]; ["bar"]])
    let result = (read reader (ctorManySimple typedefof<string>)) :?> string seq
    areSeqEqual ["foo"; "bar"] result

[<Fact>]
let ``one verbose record``() =
    let d = System.DateTime.Now
    let g = System.Guid.NewGuid()

    let reader = 
        new LocalDataReader(
            [[
                "foobar"
                true
                5uy
                'g'
                d
                1.23M
                1.45
                1.67
                g
                2s
                3
                4L
                0
            ]]
        )

    let result = (read reader (ctorVerboseRecord Single)) :?> VerboseRecord
    let e = {
        String = "foobar"
        Bool = true
        Byte = 5uy
        Char = 'g'
        DateTime = d
        Decimal = 1.23M
        Double = 1.45
        Float = 1.67
        Guid = g
        Int16 = 2s
        Int32 = 3
        Int64 = 4L
        UnionEnum = UnionEnum.One
    }

    Assert.Equal(e, result)

[<Fact>]
let ``many verbose record``() =
    let d = System.DateTime.Now
    let g = System.Guid.NewGuid()

    let reader = 
        new LocalDataReader(
            [[
                "foobar"
                true
                5uy
                'g'
                d
                1.23M
                1.45
                1.67
                g
                2s
                3
                4L
                1
            ]; [
                "foobar2"
                false
                6uy
                'd'
                d
                1.56M
                1.78
                1.90
                g
                3s
                4
                5L
                2
            ]]
        )

    let result = (read reader (ctorVerboseRecord Many)) :?> VerboseRecord seq
    let e = [
        {
            String = "foobar"
            Bool = true
            Byte = 5uy
            Char = 'g'
            DateTime = d
            Decimal = 1.23M
            Double = 1.45
            Float = 1.67
            Guid = g
            Int16 = 2s
            Int32 = 3
            Int64 = 4L
            UnionEnum = UnionEnum.Two
        }; {
            String = "foobar2"
            Bool = false
            Byte = 6uy
            Char = 'd'
            DateTime = d
            Decimal = 1.56M
            Double = 1.78
            Float = 1.90
            Guid = g
            Int16 = 3s
            Int32 = 4
            Int64 = 5L
            UnionEnum = UnionEnum.Three
        }
    ]

    areSeqEqual e result

[<Fact>]
let ``single record``() =

    let reader = 
        new LocalDataReader([["first"]])

    let result = (read reader (ctorName Single)) :?> Name

    Assert.Equal({Value = "first"}, result)

[<Fact>]
let ``single record multiple values throws``() =

    let reader = 
        new LocalDataReader([["first"]; ["second"]])

    let e = Assert.Throws<System.InvalidOperationException>(fun () -> (read reader (ctorName Single)) |> ignore)

    Assert.Equal("Sequence contains more than one element", e.Message)

[<Fact>]
let ``single record no values throws``() =

    let reader = 
        new LocalDataReader([])

    let e = Assert.Throws<System.InvalidOperationException>(fun () -> (read reader (ctorName Single)) |> ignore)

    Assert.Equal("Sequence contains no elements", e.Message)

let ``singleOrDefault record``() =

    let reader = 
        new LocalDataReader([["first"]])

    let result = (read reader (ctorName SingleOrDefault)) :?> Name

    Assert.Equal({Value = "first"}, result)

[<Fact>]
let ``singleOrDefault record multiple values throws``() =

    let reader = 
        new LocalDataReader([["first"]; ["second"]])

    let e = Assert.Throws<System.InvalidOperationException>(fun () -> (read reader (ctorName SingleOrDefault)) |> ignore)

    Assert.Equal("Sequence contains more than one element", e.Message)

[<Fact>]
let ``singleOrDefault record no values defaults``() =

    let reader = 
        new LocalDataReader([])

    let result = (read reader (ctorName SingleOrDefault))

    Assert.Equal(null, result)

[<Fact>]
let ``option some``() =

    let reader = 
        new LocalDataReader([["first"]])

    let result = (read reader (ctorOptionNameFull Single)) :?> OptionName

    Assert.Equal({OptionValue = Some "first"}, result)

[<Fact>]
let ``option none from dbnull``() =

    let reader = 
        new LocalDataReader([[System.DBNull.Value]])

    let result = (read reader (ctorOptionNameFull Single)) :?> OptionName

    Assert.Equal({OptionValue = None}, result)

[<Fact>]
let ``option none from null``() =

    let reader = 
        new LocalDataReader([[null]])

    let result = (read reader (ctorOptionNameFull Single)) :?> OptionName

    Assert.Equal({OptionValue = None}, result)

open System.Linq.Expressions

[<Fact>]
let ``lambda int add``() =

    let reader = 
        new LocalDataReader([[5; 2]])

    let p1 = Expression.Parameter(typedefof<int>)
    let p2 = Expression.Parameter(typedefof<int>)
    let lambda = Expression.Lambda(Expression.Add(p1, p2), [p1; p2])

    let ctor = 
        {
            ReturnType = Single
            Type = typedefof<int>
            TypeOrLambda = TypeOrLambdaConstructionInfo.Lambda {
                Lambda = lambda
                Parameters = [Value 0; Value 1]
            }
            PostProcess = None
        }
        
    let result = (read reader ctor) :?> int

    Assert.Equal(7, result)

[<Fact>]
let ``lambda string mod``() =

    let reader = 
        new LocalDataReader([["foo"]])

    let lambda = toLambda <@ fun f -> f + "bar"@> 

    let ctor = 
        {
            ReturnType = Single
            Type = typedefof<string>
            TypeOrLambda = TypeOrLambdaConstructionInfo.Lambda {
                Lambda = lambda
                Parameters = [Value 0]
            }
            PostProcess = None
        }

    let result = (read reader ctor) :?> string

    Assert.Equal("foobar", result)

[<Fact>]
let ``lambda post process single``() =

    let reader = 
        new LocalDataReader([["foo"]; ["bar"]; ["baz"]])

    let lambda = toLambda <@ fun (vals : string seq) -> vals |> String.concat(" ") @> 

    let ctor = 
        {
            ReturnType = Single
            Type = typedefof<string>
            TypeOrLambda = TypeOrLambdaConstructionInfo.Type {
                Type = typedefof<string>
                ConstructorArgs = [Value 0]
                PropertySets = []
            }
            PostProcess = Some lambda
        }

    let result = (read reader ctor) :?> string

    Assert.Equal("foo bar baz", result)

[<Fact>]
let ``lambda post process many``() =

    let reader = 
        new LocalDataReader([["foo"]; ["bar"]; ["baz"]])

    let lambda = toLambda <@ fun (vals : string seq) -> [vals |> String.concat(" "); vals |> String.concat(" ")] @> 

    let ctor = 
        {
            ReturnType = Many
            Type = typedefof<string>
            TypeOrLambda = TypeOrLambdaConstructionInfo.Type {
                Type = typedefof<string>
                ConstructorArgs = [Value 0]
                PropertySets = []
            }
            PostProcess = Some lambda
        }

    let result = (read reader ctor) :?> string seq

    areSeqEqual ["foo bar baz"; "foo bar baz"] result

[<Fact>]
let ``lambda type mod``() =

    let reader = 
        new LocalDataReader([["foo"]])

    let lambda = toLambda <@ fun ( opt : OptionName )  -> opt.OptionValue.Value + "bar"@> 

    let ctor = 
        {
            ReturnType = Single
            Type = typedefof<string>
            TypeOrLambda = TypeOrLambdaConstructionInfo.Lambda {
                Lambda = lambda
                Parameters = [Type (ctorOptionName)]
            }
            PostProcess = None
        }

    let result = (read reader ctor) :?> string

    Assert.Equal("foobar", result)

//
//[<Fact>]
//let ``group``() =
//
////    let reader = 
////        new LocalDataReader
////            ([
////                ["one"; "foo"]
////                ["one"; "bar"]
////                ["two"; "baz"]
////            ])
////
////    let ctor = 
////        {
////            ReturnType = Single
////            Type = typedefof<string>
////            TypeOrLambda = TypeOrLambdaConstructionInfo.Type ctorKeyValuePair
////        }
//
//    let raw = [
//        { Key = "one"; KeyValue = "foo" }
//        { Key = "one"; KeyValue = "bar" }
//        { Key = "two"; KeyValue = "baz" }
//    ]
//    let result = System.Linq.Enumerable.GroupBy(raw, (fun x -> x.Key))
//    printfn "%A" result
//    //let result = (read reader ctor) :?> seq<System.Linq.IGrouping<string, KeyValuePair>>
//
//    Assert.Equal(2, result |> Seq.length)
//    let first = result |> Seq.head
//    let _second = result |> Seq.last
//
//    Assert.Equal("one", first.Key)
//    areSeqEqual 
//        [
//            { Key = "one"; KeyValue = "foo" }
//            { Key = "one"; KeyValue = "bar" }
//        ]
//        (first |> Seq.toList)
//
//    Assert.Equal("two", first.Key)
//    areSeqEqual 
//        [
//            { Key = "two"; KeyValue = "baz" }
//        ]
//        (first |> Seq.toList)
