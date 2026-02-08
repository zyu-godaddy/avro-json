
# tmp

A software system
- ingest events
- define functions over events
- order events for functions
- compute functions over ordered events

## events

inbound events -- originated at external world
that enter the system at boundaries.

outbound events -- they are merely results of some functions.
There are components monitoring emergence of these events and
sending them to external world.
In this article, such components are not considered part
of the system, but rather a companion system.
"Sending" itself is often an interesting external event
that should be ingested by the system.

internal events -- they are merely results of some intermediary functions
upon which other functions are defined.


**In the rest of the article, events only refer to inbound events**
unless qualified otherwise.

Event intrinsics: all info attached to the event by external world,
as well as ingestion site and time at system boundary.

## functions

Functions are the 'ontology' of the system;
they are defined over events (in some structures) (but nothing else).

It is often insufficient to use _set_ of events as function domain.

Function may need a structure over events. (e.g. sequence/tree)
We could construct the structure based only on event intrinsics;
however, there might be some sense of inconsistency between 2 
structures constructed on 2 sets of events.

The most common/important structure is event ordering.
It is possible to establish an order from event intrinsics
that is consistent. But for computation visibility of events,
we still need physical process to order events.
Therefore it's best that the order structure is provided to function.


Often, functions are defined in a way where orders among some events
are important. If order of 2 events are switched,
function result is different (by some criteria (todo)).

We are going to order all events in a DAG. 
While the DAG contains the full structure of ordering,
it is too much. 
Instead, we require that all functions take a _sequence_ of events, which is 
a topological order of the DAG.
We require that all functions yield the same results on 
_any_ topological order of the DAG.
This requires careful definition of functions and careful ordering of events.

(Do we need other structures over events? Or it's up to each function to establish
structures that they need -- unless 2 functions need to agree on a shared structure)

## event ordering

This is a partial order and forms a DAG.

Observer consistency:
- r sees ej => r sees any ei that ei<ej
- r1 sees e => r2 sees e. (same observer)

Rendezvous point: two events that should be ordered must 
reach the rendezvous point (a physical location) to be ordered,
before they are read by all other observers.

Kafka: 

Global ordering -- possible. but what about compute visibility consistency.
Observer may need to buffer an event until it's certain that 
all events ordered before it has reached the observer.
Example: observer sees e1 from (s1,t1). It has to wait to make sure
no more e0 from (s0,t0) can arrive. This can be assisted by s0 emitting
heartbeats.

New events are added to frontier of DAG;
observers read new events from frontier, guarantee topo order.

Accidental ordering: two events are ordered when they don't need to be.
If a new function is defined that requires their ordering, 
the previously established order may not be correct.

Puzzle: order all events for an account.
order all events for an inventory.
how to order events involve (account, inventory).
another example: how to order interactions between two accounts (a1, a2).

todo: external order; events already ordered when ingested.

todo: causality of events that's natural to external observers, 
consistent with real-world causality.
write-ack-write, w2 must be ordered after w1. what about r2?

## compute

a physical process at some space and time range.
It takes time.
It is stateful, imperative.

## event/function first -- state/compute first

## partition

## transactional database

good/easy for simple app. In the context of this article, it does:

define ontology of system

order events. (dedup events; reject bad-formed events)

perform state transition

store latest state. can emit state change events.

typically lose event history

A state/compute first approach, not event/function first approach

## archive

store events and event ordering.

cold storage.

delete history?

## app evolution

change of functions? change of ordering?

## etc

query = functions. 

function composition: f(e) = x(y1(e), y2(e))
compute derived results, based on 1 or more sources.
changes from source data.
concurrent/partition

