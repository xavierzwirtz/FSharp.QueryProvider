[<AutoOpen>]
module AssertExtensions

let compareSeq a b = 
    if Seq.length a = Seq.length b then
        Seq.fold (&&) true (Seq.zip a b |> Seq.map (fun (aa,bb) -> aa=bb))
    else
        false
    
let areSeqEqual e a = 
    if not (compareSeq e a) then
        let message = sprintf "Expected: %A \nActual: %A" (e |> Seq.toList) (a |> Seq.toList)
        raise (Xunit.Sdk.AssertActualExpectedException(e, a, message))