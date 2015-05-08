(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin"

(**
FSharp.QueryProvider
======================

Modular Linq provider library for .Net. Use it to easily add a Linq provider to your orm.

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The FSharp.QueryProvider library can be <a href="https://nuget.org/packages/FSharp.QueryProvider">installed from NuGet</a>:
      <pre>PM> Install-Package FSharp.QueryProvider</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Example
-------

Example for doing a like query against a simple person type.

*)
#I "../../bin"
#r "FSharp.QueryProvider/FSharp.QueryProvider.dll"
open FSharp.QueryProvider.QueryTranslator
open FSharp.QueryProvider.Queryable
open System.Data

let connectionString = "Server=localhost;Database=Soma.Core.IT;Trusted_Connection=True;"

let queryProvider =
    DBQueryProvider (
        (fun () -> new SqlClient.SqlConnection(connectionString)), 
        (fun connection expression ->
            let command, ctor = translateToCommand QueryDialect.SqlServer2012 SelectQuery None None None connection expression
            let ctor =
                match ctor with
                | Some ctor -> ctor
                | None -> failwith "no ctorinfor generated"
            command :> IDbCommand, ctor), 
        None, 
        None)

type JobKind =
| Salesman = 0
| Manager = 1

type Person =
    { PersonId : int
      PersonName : string
      JobKind : JobKind
      VersionNo : int }

let does = query {
    for p in makeQuery<Person>(queryProvider) do
    where(p.PersonName.Contains "doe")
    select p
}

printfn "%A" (does |> Seq.toList)

(**
Some more info

Samples & documentation
-----------------------

The library comes with comprehensible documentation. 
It can include tutorials automatically generated from `*.fsx` files in [the content folder][content]. 
The API reference is automatically generated from Markdown comments in the library implementation.

 * [Tutorial](tutorial.html) contains a further explanation of this sample library.

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding a new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read the [library design notes][readme] to understand how it works.

The library is available under Public Domain license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/fsprojects/FSharp.QueryProvider/tree/master/docs/content
  [gh]: https://github.com/fsprojects/FSharp.QueryProvider
  [issues]: https://github.com/fsprojects/FSharp.QueryProvider/issues
  [readme]: https://github.com/fsprojects/FSharp.QueryProvider/blob/master/README.md
  [license]: https://github.com/fsprojects/FSharp.QueryProvider/blob/master/LICENSE.txt
*)
