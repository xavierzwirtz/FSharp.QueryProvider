module DataReaderTest
open NUnit.Framework
open FSharp.QueryProvider
open FSharp.QueryProvider.DataReader

open Models
open LocalDataReader

type SimpleRecord = {
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
}

let ctorOneSimple t = 
    {
        ManyOrOne = One
        Type = t
        ConstructorArgs = [0;]
        PropertySets = []
    }

let ctorManySimple t = 
    {
        ManyOrOne = Many
        Type = t
        ConstructorArgs = [0]
        PropertySets = []
    }

let ctorSimpleRecord manyOrOne = 
    {
        ManyOrOne = manyOrOne
        Type = typedefof<SimpleRecord>
        ConstructorArgs = [0..11]
        PropertySets = []
    }
    
[<Test>]
let ``one string``() =
    let reader = new LocalDataReader([["foo bar"]])
    let result = (read reader (ctorOneSimple typedefof<string>)) :?> string
    Assert.AreEqual("foo bar", result)
[<Test>]
let ``one bool true``() =
    let reader = new LocalDataReader([[true]])
    let result = (read reader (ctorOneSimple typedefof<bool>)) :?> bool
    Assert.AreEqual(true, result)
[<Test>]
let ``one bool false``() =
    let reader = new LocalDataReader([[false]])
    let result = (read reader (ctorOneSimple typedefof<bool>)) :?> bool
    Assert.AreEqual(false , result)
//[<Test>]
//let ``one byte``() =
//    let reader = new LocalDataReader([[5y]])
//    let result = (read reader (ctorOneSimple typedefof<sbyte>)) :?> sbyte
//    Assert.AreEqual(5y, result)
[<Test>]
let ``one ubyte``() =
    let reader = new LocalDataReader([[5uy]])
    let result = (read reader (ctorOneSimple typedefof<byte>)) :?> byte
    Assert.AreEqual(5uy, result)
[<Test>]
let ``one char``() =
    let reader = new LocalDataReader([['g']])
    let result = (read reader (ctorOneSimple typedefof<char>)) :?> char
    Assert.AreEqual('g' , result)
[<Test>]
let ``one System.DateTime``() =
    let d = System.DateTime.Now
    let reader = new LocalDataReader([[d]])
    let result = (read reader (ctorOneSimple typedefof<System.DateTime>)) :?> System.DateTime
    Assert.AreEqual(d, result)
[<Test>]
let ``one decimal``() =
    let reader = new LocalDataReader([[1.23m]])
    let result = (read reader (ctorOneSimple typedefof<decimal>)) :?> decimal
    Assert.AreEqual(1.23m, result)
[<Test>]
let ``one double``() =
    let reader = new LocalDataReader([[4.14]])
    let result = (read reader (ctorOneSimple typedefof<double>)) :?> double
    Assert.AreEqual(4.14, result)
[<Test>]
let ``one float``() =
    let reader = new LocalDataReader([[4.14]])
    let result = (read reader (ctorOneSimple typedefof<float>)) :?> float
    Assert.AreEqual(4.14, result)
[<Test>]
let ``one System.Guid``() =
    let g = System.Guid.NewGuid()
    let reader = new LocalDataReader([[g]])
    let result = (read reader (ctorOneSimple typedefof<System.Guid>)) :?> System.Guid
    Assert.AreEqual(g, result)
[<Test>]
let ``one int16``() =
    let reader = new LocalDataReader([[86s]])
    let result = (read reader (ctorOneSimple typedefof<int16>)) :?> int16
    Assert.AreEqual(86s, result)
[<Test>]
let ``one int32``() =
    let reader = new LocalDataReader([[5]])
    let result = (read reader (ctorOneSimple typedefof<int32>)) :?> int32
    Assert.AreEqual(5, result)
[<Test>]
let ``one int64``() =
    let reader = new LocalDataReader([[5L]])
    let result = (read reader (ctorOneSimple typedefof<int64>)) :?> int64
    Assert.AreEqual(5L, result)

//no need to test the many for every type
[<Test>]
let ``many string``() =
    let reader = new LocalDataReader([["foo"]; ["bar"]])
    let result = (read reader (ctorManySimple typedefof<string>)) :?> string seq
    areSeqEqual ["foo"; "bar"] result

[<Test>]
let ``one record``() =
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
            ]]
        )

    let result = (read reader (ctorSimpleRecord One)) :?> SimpleRecord
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
    }

    Assert.AreEqual(e, result)

[<Test>]
let ``many record``() =
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
            ]]
        )

    let result = (read reader (ctorSimpleRecord Many)) :?> SimpleRecord seq
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
        }
    ]

    areSeqEqual e result