#r "nuget: Akka.FSharp"

module Chord = 
    open Akka
    open Akka.FSharp
    open Akka.Actor
    open FSharp.Collections
    open System
    open System.Security.Cryptography

    // Types of messages that can be sent or received by "peer" actors
    type PeerMessage =
        | CreatePeer of (int * IActorRef) //message used to create a Chord peer node
        | Join of (int * IActorRef * int * IActorRef) //message used to join an existing Chord network
        | KeyLookup of (int * int * IActorRef) // message used to perform a key Keylookup
        | AddToFingerTable of int  //message used to add an entry to the finger table
        | Done //message used as a marker or signal to indicate that a certain task or operation has been completed

    //  Types of messages that can be sent to or received by the Simulator actor
    type SimulatorMessage = 
        | CreateChord //message used to create a Chord simulator node
        | JoinChord //message used to coordinate the joining of Chord nodes in the network.
        | MakeFingerTable // message used to trigger the generation of the finger table
        | Lookups //message used to initiate key lookups
        | KeyFound of (IActorRef * int * int * IActorRef) //message sent when a key Keylookup operation successfully finds the desired key

    // used to create an instance of an actor system named "System."
    let systemReference = ActorSystem.Create("System")

    let mutable m = 0
    
    // generate SHA-1 hash for a key
    let generateHashKey(key: int) =
        let bytesArr = BitConverter.GetBytes(key)
        let hash = HashAlgorithm.Create("SHA1").ComputeHash(bytesArr)
        BitConverter.ToString(hash).Replace("-", "").ToLower()

    let mutable peers = [||]
    let mutable flag = true
    let mutable searches = 0

    // Peer Actor
    let peer (id: int) (peerMailbox:Actor<_>) =
        
        // Peer State
        let mutable PeerId: int = id;
        let mutable Successor: (int * IActorRef)=(-1, null);
        let mutable Predecessor: (int * IActorRef)=(-1, null);
        // reference to an actor in the Chord simulation that serves as the coordinator or master for this Chord peer
        let mutable SimulatorReference: IActorRef = null;
        let mutable PeerFingerTable: (int * IActorRef) []=[||];
        
        // function to continually process messages and take appropriate actions based on the message content
        let rec loop state = actor {
            
            let! message = peerMailbox.Receive()
            
            match message with
            | CreatePeer (id,master)-> 
                SimulatorReference <- master
                PeerId <- id
            | Join (suci, sucR, prei, preR)->
                Predecessor <- (prei, preR)
                Successor <- (suci, sucR)
            | KeyLookup (key, hops, requestor) ->
                let mutable hopCount = hops
                let mutable ftfound = false

                if flag && key = PeerId  then 
                    flag <- false
                    SimulatorReference <! KeyFound((snd Successor), key, hopCount, requestor)
                    peerMailbox.Self <! Done
                elif (key <= (fst Successor)) && (key > PeerId) then
                    flag <- false
                    SimulatorReference <! KeyFound((snd Successor), key, hopCount, requestor)
                    peerMailbox.Self <! Done
                else
                    if  flag && PeerId > (fst Successor) then 
                        hopCount <- hopCount + 1 
                        flag <- false
                        SimulatorReference <! KeyFound((snd (Array.get peers (0))), key, hopCount, requestor)
                        peerMailbox.Self <! Done
                    else 
                        let mutable i = m - 1
                        while flag &&  i > 0 do
                            let fingerValue = (fst (Array.get PeerFingerTable i))
                            if  flag && fingerValue < PeerId then
                                flag <- false
                                hopCount <- hopCount+1
                                SimulatorReference <! KeyFound((snd (Array.get peers (0))), key, hopCount, requestor)
                            elif flag && (fingerValue <= key && fingerValue > PeerId)  then
                                ftfound <- true
                                hopCount <- hopCount + 1
                                (snd (Array.get PeerFingerTable i)) <! KeyLookup(key, hopCount, requestor)
                            i <- 0
                            Threading.Thread.Sleep(400)
                            hopCount <- hopCount + 1
                            (snd (Array.get PeerFingerTable (m - 1))) <! KeyLookup(key, hopCount, requestor)
                peerMailbox.Self <! Done
            | AddToFingerTable(id) ->
                for i in 0..m - 1 do 
                    let mutable ft = false
                    for j in 0..peers.Length - 2 do 
                        let ele = (fst (Array.get peers j))
                        let scnd = (fst (Array.get peers (j+1)))
                        if ((id + (pown 2 i)) > ele) && ((id + (pown 2 i)) <= scnd) then
                            PeerFingerTable <- Array.append PeerFingerTable [|(scnd , snd (Array.get peers (j+1)))|]
                            ft <- true
                    if not ft  then
                        //printfn "%A points to %A" (id+(pown 2 i)) 0 
                        PeerFingerTable <- Array.append PeerFingerTable [|(fst (Array.get peers (0)), snd (Array.get peers (0)))|]
            | Done -> printf "" |> ignore
            | _ ->  failwith "[ERR] Unknown error." |> ignore

            return! loop()
        }

        loop ()

    let mutable HopsSum =0
    let mutable averageHops= 0.0

    // Simulator Actor
    let simulator (numNodes: int) (numRequests: int) (simulatorMailbox:Actor<_>) =

        // calculates and tracks statistics on the average number of hops taken during key lookups
        let rec loop state = actor {
            let! message = simulatorMailbox.Receive()
            let mutable maxid=0
            let r = Random()

            match message with
            | CreateChord ->
                m <- (Math.Log2(float numNodes) |> int)
                for i in 1..numNodes do
                    let id = r.Next(maxid + 1, (maxid + (Math.Log (numNodes |> float, 2.0)  |> int)))
                    maxid <- id
                    let node  = spawn systemReference ("Node" + (generateHashKey id)) (peer(maxid))
                    peers <- Array.append peers [|(maxid, node)|]
                    node <! CreatePeer (maxid, simulatorMailbox.Self)
                simulatorMailbox.Self <! JoinChord
            | JoinChord ->
                for i in 0..numNodes - 1 do 
                    let mutable successori = i + 1
                    let mutable predecessori = i - 1
                    let node = Array.get peers i
                    if i = (numNodes - 1) then 
                        successori <- 0
                    if i = 0 then 
                        predecessori <- (numNodes - 1)
                    let sucR = Array.get peers (successori)
                    let preR = Array.get peers (predecessori)
                    snd node <! Join (fst sucR, snd sucR, fst preR, snd preR)
                    Threading.Thread.Sleep(150)
                simulatorMailbox.Self <! MakeFingerTable
            | MakeFingerTable ->
                for i in 0..numNodes - 1 do 
                    let x = Array.get peers (i)
                    snd x <! AddToFingerTable(fst x)
                    Threading.Thread.Sleep(150)
                simulatorMailbox.Self <! Lookups 
            | Lookups ->
                let mutable key = 0
                for i in 0..numNodes - 1 do
                    key <- key + (fst (Array.get peers i))
                key <- key / peers.Length
                key <- key |> int
                for i in 0..numNodes - 1 do
                    for j in 1..(numRequests) do
                        (snd (Array.get peers i)) <!  KeyLookup (fst(Array.get peers (numNodes - 1)) - 1, 0, (snd (Array.get peers i)))
                        flag <- true
                        Threading.Thread.Sleep(1000)
            | KeyFound (ref, key, hopCount, requestor) ->
                HopsSum <- HopsSum + hopCount
                averageHops <- (HopsSum |> float) / (numNodes  * (numRequests) |> float) 
                if searches = (numNodes * numRequests) then
                    printfn "Average number of hops:%A" averageHops
                    systemReference.Terminate()
                searches <- searches + 1
            | _ ->  failwith "[ERR] Unknown error." |> ignore
            
            return! loop()
        }

        loop ()


    // Start of the program
    let startChordSimulation numNodes numRequests =
        let simulatorRef = spawn systemReference "simulator" (simulator(numNodes) (numRequests)) 
        simulatorRef <! CreateChord
        
        Threading.Thread.Sleep(9000)
        Threading.Thread.Sleep(9000)

        systemReference.WhenTerminated.Wait()
       
        