# DCP Sequence Diagrams

This document shows the sequence of calls between the major DCP components for
the main DCP operations.

## Notifying DCP on a new sequence number
```mermaid
sequenceDiagram
    autonumber
    actor Frontend Thread
    actor DCP Thread
    participant Producer
    participant ActiveStream
    actor Task as Active Stream<br>Checkpoint Processor Task

    note right of Frontend Thread: Mutation occurs
    Frontend Thread->>Frontend Thread: VBucket::set()
    Frontend Thread->>Frontend Thread: DcpConnMap::notifyVBConnections()
    Frontend Thread->>+Producer: notifySeqnoAvailable(vbid)
    rect rgb(228, 228, 228)
    loop For each ActiveStream matching vbid
        Producer->>+ActiveStream: notifySeqnoAvailable()
        ActiveStream->>ActiveStream: notifyStreamReady(force=false)
        rect rgb(218, 218, 218)
        opt If itemsReady was false or force
            ActiveStream->>+Producer: notifyStreamReady(vb)
            Producer->>Producer: VBReadyQueue::pushUnique(vb)
            rect rgb(208, 208, 208)
            opt Transitioned from empty to non-empty readyQ
                Producer-)DCP Thread: scheduleDcpStep()
            end
            end
        Producer-->>-ActiveStream:notifyStreamReady(vb)
        end
        end
    ActiveStream-->>-Producer: notifySeqnoAvailable()
    end
    end
    Producer-->>-Frontend Thread: notifySeqnoAvailable()
```

## DCP Front-end thread waking up after notification
```mermaid
sequenceDiagram
    autonumber
    actor Frontend Thread
    actor DCP Thread
    participant Producer
    participant ActiveStream
    actor Task as Active Stream<br>Checkpoint Processor Task

    note over DCP Thread,Producer: DCP Thread wakes up this connection
    DCP Thread->>+Producer: step()
    Producer->>Producer: getNextItem()
    Producer->>+ActiveStream: next()
    alt InMemory
        ActiveStream->>ActiveStream: inMemoryPhase()
        alt readyQ is empty
            ActiveStream->>ActiveStream: nextCheckpointItem()
            alt CheckpointManager has items
                ActiveStream->>+Producer: scheduleCheckpointProcessorTask()
                Producer->>Producer: ActiveStreamCheckpointProcessorTask::schedule(ActiveStream&)
                alt If vBucket not already in queue
                    Producer-)Task: wakeup()
                end
                Producer-->>-ActiveStream: scheduleCheckpointProcessorTask()
            end
        end
        ActiveStream->>ActiveStream: nextQueuedItem()
    else Backfilling
        ActiveStream->>ActiveStream: backfillPhase()
    end
    ActiveStream-->>ActiveStream: itemsReady.store(response ? true : false);
    ActiveStream-->>-Producer: next()
    Producer-->>-DCP Thread: step()
```

## ActiveStreamCheckpointProcessorTask processing checkpoints
```mermaid
sequenceDiagram
    actor Frontend Thread
    actor DCP Thread
    participant ActiveStream
    participant Producer
    actor Task as Active Stream<br>Checkpoint Processor Task

    note over Task: NonIO thread schedules Task
    Task->>+ActiveStream: nextCheckpointItemTask()
        activate ActiveStream
        ActiveStream->>ActiveStream: getOutstandingItems()
        ActiveStream->>ActiveStream: chkptItemsExtractionInProgress.store(true)
        deactivate ActiveStream
        activate ActiveStream
        ActiveStream->>ActiveStream: processItems()
        ActiveStream->>ActiveStream: chkptItemsExtractionInProgress.store(false);
        deactivate ActiveStream

        ActiveStream->>ActiveStream: notifyStreamReady(force=true)
        ActiveStream->>+Producer: notifyStreamReady(vb)
            Producer->>Producer: VBReadyQueue::pushUnique(vb)
            Producer->>Producer: BufferLog::unpauseIfSpaceAvailable()
            Producer-)Frontend Thread: scheduleDcpStep()
            Producer-->>-ActiveStream:notifyStreamReady(vb)
        ActiveStream-->>-Task: nextCheckpointItemTask
```
