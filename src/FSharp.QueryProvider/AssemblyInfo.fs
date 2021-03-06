﻿namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("FSharp.QueryProvider")>]
[<assembly: AssemblyProductAttribute("FSharp.QueryProvider")>]
[<assembly: AssemblyDescriptionAttribute("Makes implementing IQueryable and IQueryProvider in F# functional and easy.")>]
[<assembly: AssemblyVersionAttribute("1.0.7")>]
[<assembly: AssemblyFileVersionAttribute("1.0.7")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0.7"
