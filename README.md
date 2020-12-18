# GroupMutex

GroupMutex allows locks to be secured by a group allowing concurrent access only within the group.
Suppose if lock(1) is called, any further calls to lock(1) (before it unlocks), won't block.
If lock(2) is called concurrently, lock(2) has to wait till it gets it's time to process.
When lock(2) is obtained, it is guaranteed that no other group holds the lock

# Usage

```go
package main

import "github.com/nnanto/gmutex"

func main() {
	gm := gmutex.New(3)
	for i := 0; i < 100; i++ {
		go worker1(gm)
		go worker2(gm)
	}
	//...
}

func worker1(gm *gmutex.GM) {
	gm.Lock(1)
	// concurrent access only by members holding lock(1)
	gm.Unlock(1)
}

func worker2(gm *gmutex.GM) {
	gm.Lock(2)
	// concurrent access only by members holding lock(2)
	gm.Unlock(2)
}

```

# Under the hood

The mechanism is based on the following state transition of broker:

Empty(0) --_Lock(g)_--> Group(g) --_Unlock(g)_--> PrepareUnlock(-1) --> Empty(0) 

1. Initially, brokerState is set to 'Empty(0)'
1. When Lock(g) is called, the first call to successfully CompareAndSwap changes the brokerState to 'g', while others acquire semaphores & wait
1. After obtaining lock, all waiting locks on 'g' are released
1. First Unlock(g) to reach '0' left over tasks will switch the brokerState to 'PrepareUnlock(-1)'. 
1. In this phase, the broker waits for all pending tasks that 'g' is currently working on and then switches to 'Empty(0)' state again
1. Once switched back, the unlocking routine will start any 'g' that is waiting for lock
