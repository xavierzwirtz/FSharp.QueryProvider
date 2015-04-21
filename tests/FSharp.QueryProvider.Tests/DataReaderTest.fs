module DataReaderTest
open NUnit.Framework
open FSharp.QueryProvider
open FSharp.QueryProvider.QueryTranslator

open Models
open LocalDataReader

[<Test>]
let ``LocalDataReader``() =
    let reader = new LocalDataReader([[5]]) :> System.Data.IDataReader

    Assert.IsTrue(reader.Read())
    Assert.AreEqual(reader.GetInt32(0), 5)
    Assert.IsFalse(reader.Read())

//let ``int``() =
//    let reader = new LocalDataReader([[5]])
//
//    let result = (DataReader.read reader ctorInfo) :?> int
//
//    Assert.AreEqual(result, 5)
//
//let ``bool true``() =
//    let reader = new LocalDataReader([[true]])
//
//    let result = (DataReader.read reader ctorInfo) :?> bool
//
//    Assert.IsTrue(result)
//
//let ``bool false``() =
//    let reader = new LocalDataReader([[false]])
//
//    let result = (DataReader.read reader ctorInfo) :?> bool
//
//    Assert.IsTrue(result)