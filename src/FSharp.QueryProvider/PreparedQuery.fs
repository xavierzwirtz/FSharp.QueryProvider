module FSharp.QueryProvider.PreparedQuery

open FSharp.QueryProvider.DataReader

type PreparedParameter<'T> = {
    Name : string
    Value : obj 
    DbType : 'T
}

type PreparedStatement<'P> = {
    Text : string
    FormattedText : string
    Parameters : PreparedParameter<'P> seq
    ResultConstructionInfo : TypeConstructionInfo
}