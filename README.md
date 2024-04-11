# M5: Distributed Execution Engine
> Full name: Seong-Heon Jung
> Email:  seong-heon_jung@brown.edu
> Username:  sjung35

## Summary
> Summarize your implementation, including key challenges you encountered

My implementation comprises 2 new software components, totaling 250 added lines of code over the previous implementation. Key challenges included `<1, 2, 3 + how you solved them>`.

## Correctness & Performance Characterization
> Describe how you characterized the correctness and performance of your implementation

*Correctness*:
In addition to the end-to-end mapreduce tests, I used asserts generously throughout
my code to ensure that each intermediate state does not violate any invariants or expectations.
Furthermore, I used JSDoc to document my code, thus providing soft type saftey guarantees.

*Performance*:

## Key Feature
> Which extra features did you implement and how?
I implemented distributed persistence. When the map workers finish computation, it pools the produced results.
These results are then immediately placed on the nodes which will run the reduce computation via `store:put`.
The supervisor/coordinator does not access the intermediate results of the computation, which reduces the load on the supervisor.

## Time to Complete
> Roughly, how many hours did this milestone take you to complete?

Hours: 20

