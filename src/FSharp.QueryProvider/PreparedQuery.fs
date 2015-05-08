module FSharp.QueryProvider.PreparedQuery

open FSharp.QueryProvider.DataReader

/// <summary>
/// Represents a query parameter
/// </summary>
type PreparedParameter<'T> = {
    Name : string
    Value : obj 
    DbType : 'T
}

/// <summary>
/// Represents everything needed to create a IDbCommand and construct something from its reader.
/// </summary>
type PreparedStatement<'P> = {
    Text : string
    FormattedText : string
    Parameters : PreparedParameter<'P> seq
    ResultConstructionInfo : ConstructionInfo option
}