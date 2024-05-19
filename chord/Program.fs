open System
open Chord
// Entry point for your program
[<EntryPoint>]
let main argv =
    if argv.Length = 2 then
        
        Chord.startChordSimulation (int argv.[0]) (int argv.[1])
    else
       // failwith "Incorrect number of inputs"
        printfn "Error: Incorrect number of inputs"
        exit 1 // Use a non-zero exit code to indicate an error


    0 // Replace this with an appropriate exit code

