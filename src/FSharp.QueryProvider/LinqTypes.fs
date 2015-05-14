module FSharp.QueryProvider.LinqTypes

open System.Linq
type Grouping<'TKey, 'TElement>(key : 'TKey, values : seq<'TElement>) =
    interface IGrouping<'TKey, 'TElement> with
        member x.GetEnumerator(): System.Collections.Generic.IEnumerator<'TElement> = 
            (values :> System.Collections.Generic.IEnumerable<'TElement>).GetEnumerator()
        
        member x.GetEnumerator(): System.Collections.IEnumerator = 
            (values :> System.Collections.IEnumerable).GetEnumerator()
        
        member x.Key: 'TKey = 
            key