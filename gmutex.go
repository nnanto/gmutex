package gmutex

import (
	"sync/atomic"
	_ "unsafe"
)

//go:linkname acquire runtime.semacquire
func acquire(s *uint32)

//go:linkname release runtime.semrelease
func release(s *uint32)

//go:linkname fastrandn runtime.fastrandn
func fastrandn(n uint32) uint32

const (
	allDone       int32 = -2 // Notifies that unlock succeeded
	prepareUnlock int32 = -1 // Indication to pause all new locks/unlocks while unlocking current group
	empty         int32 = 0  // Indicate no group might be holding lock
)
const maxCount int32 = 1 << 30

type Strategy int

const (
	ChooseMax Strategy = iota
	ChooseRandom
	ChooseFirst
)

// GM provides mutual exclusion between groups (group numbers starting from 1).
// So when lock(1) is called any further calls to lock(1) won't block.
// When lock(2) is called, lock(2) has to wait till it gets its time to process. When
// it starts, it is guaranteed that no other group holds the lock
type GM struct {
	counts        []int32      // counts hold the number of tasks (waiting/executing) for each group
	semaphores    []uint32     // semaphores for each group
	brokerState   int32        // central lock to be held by group to unlock
	brokerWaitSem uint32       // semaphore for releasing broker wait during unlock
	pending       int32        // number of pending tasks of current group
	strategy      func() int32 // strategy to choose the next group from list of waiting groups
}

// New creates a GM with ChooseMax strategy
func New(maxGroupSize int) *GM {
	g := &GM{}
	g.counts = make([]int32, maxGroupSize+1)
	g.semaphores = make([]uint32, maxGroupSize+1)
	g.strategy = g.chooseMaxCount
	return g
}

// NewWithStrategy creates a GM with defined strategy. Panics if strategy is unknown
func NewWithStrategy(maxGroupSize int, strategy Strategy) *GM {
	g := &GM{}
	g.counts = make([]int32, maxGroupSize+1)
	g.semaphores = make([]uint32, maxGroupSize+1)
	switch strategy {
	case ChooseMax:
		g.strategy = g.chooseMaxCount
	case ChooseFirst:
		g.strategy = g.chooseFirst
	case ChooseRandom:
		g.strategy = g.chooseRandom
	default:
		panic("unknown strategy")
	}
	return g
}

// Lock locks the group.  This is similar to Reader lock in sync.RWMutex i.e, it allows
// multiple Locks to same group. However it blocks, if a lock is from a different group.
func (gm *GM) Lock(group int32) {
	gm.validateGroup(group)
	if c := atomic.AddInt32(&gm.counts[group], 1); c > 0 {
		// attempt to fetch brokerState
		gm.broker(group, empty)
		acquire(&gm.semaphores[group])
	}
}

// Unlock unlocks the group. The last Unlock from the group waits for any unfinished tasks
// and gives up the lock to any pending group
func (gm *GM) Unlock(group int32) {
	gm.validateGroup(group)
	c := atomic.AddInt32(&gm.counts[group], -1)
	if c >= 0 && atomic.AddInt32(&gm.pending, -1) == 0 {
		release(&gm.brokerWaitSem)
	} else if c == -maxCount {

		x := gm.broker(prepareUnlock, group)
		if x == allDone {
			// wake up any sleeping tasks
			possiblyPendingGroup := gm.strategy()
			if possiblyPendingGroup > 0 {
				// T1                          T2
				//
				// #1 Lock(1)
				// #2 Unlock(1)                #3 Lock(2)
				//     #4 pg = strategy
				//                             #5 Unlock(2)
				//     #6 if pg > 0:
				//     ! WE ARE HERE:
				//     if we just call broker(pg,0)
				//     then we will stop indefinitely as there's no one
				//	   to unlock
				//
				// So we simulate a new entry, hence if the possiblyPendingGroup has already finished, this will
				// be a wasteful op, otherwise it'll unlock the entries waiting in possiblyPendingGroup
				// by obtaining lock (thus switching gm.brokerState from EMPTY to possiblyPendingGroup)
				// WARNING: This noopLock could pile up to size = number of groups
				gm.noopLock(possiblyPendingGroup)
			}
		}

	}
}

// broker is a central authority that allows or denies a group's lock
func (gm *GM) broker(request int32, existingGroup int32) int32 {
	if atomic.CompareAndSwapInt32(&gm.brokerState, existingGroup, request) {
		if request == prepareUnlock {
			// We try to unlock but at the same time, there might be an old request that is yet
			// to grab the lock (before line #92) so lets wait
			// Scenario:
			//    T1                              T2                                          T3
			//    #1 Lock(1)
			//    #2 Unlock(1) (c == -maxCount)
			//                                    #3 Lock(1)
			//                                    #4 Unlock(1)  (c == -maxCount)
			//                                                                                #5 Lock(1)
			//                                                                                #6 broker(1,0)
			//                                                                                #7    CAS(1,0)
			//
			//        #8 CAS(1,-1)
			//        ! WE ARE HERE: no one has lock now					  #9 grabLock(0)
			//        leaveLock() should happen after #9

			// So in above case we simulate a lock/unlock.
			if atomic.AddInt32(&gm.counts[existingGroup], 1) > 0 {
				acquire(&gm.semaphores[existingGroup])
			}
			atomic.AddInt32(&gm.counts[existingGroup], -1)
			//gm.noopLock(existingGroup)
			gm.leaveLock(existingGroup)

			// no task is waiting but there could be tasks added before CAS executes, so return allDone
			// to let the caller check for any pending groups
			if !atomic.CompareAndSwapInt32(&gm.brokerState, request, empty) {
				panic("should be able to CAS")
			}
			return allDone

		} else {
			gm.grabLock(request)
		}
	}

	return request
}

// noopLock creates a no-op Lock() Unlock() on group
func (gm *GM) noopLock(group int32) {
	gm.Lock(group)
	gm.Unlock(group)
}

// leaveLock unlocks and waits for all pending tasks to complete
func (gm *GM) leaveLock(group int32) {
	c := atomic.AddInt32(&gm.counts[group], maxCount)
	if c != 0 && atomic.AddInt32(&gm.pending, c) != 0 {
		acquire(&gm.brokerWaitSem)
	}
}

// grabLock obtains lock for group and wakes up all sleeping tasks
func (gm *GM) grabLock(group int32) {
	waitingTasks := atomic.AddInt32(&gm.counts[group], -maxCount) + maxCount
	for i := int32(0); i < waitingTasks; i++ {
		release(&gm.semaphores[group])
	}
}

// validate group input
func (gm *GM) validateGroup(group int32) {
	if group <= empty || group >= int32(len(gm.counts)) {
		panic("Lock group should be greater than 0 and less than or equal to the size defined")
	}
}

/**
Strategies for choosing group when multiple groups are waiting
*/

// chooseMaxCount chooses a group with max sleeping tasks
func (gm *GM) chooseMaxCount() int32 {
	maxVal, maxGroup := int32(0), empty
	for i := 1; i < len(gm.counts); i++ {
		if c := atomic.LoadInt32(&gm.counts[i]); c > maxVal {
			maxVal = c
			maxGroup = int32(i)
		}
	}
	return maxGroup
}

// chooseRandom chooses a random sleeping group
func (gm *GM) chooseRandom() int32 {
	chosenGroup := empty
	seen := uint32(0)
	for i := 1; i < len(gm.counts); i++ {
		if atomic.LoadInt32(&gm.counts[i]) > 0 {
			if fastrandn(seen) == 0 {
				chosenGroup = int32(i)
				seen += 1
			}
		}
	}
	return chosenGroup
}

// chooseFirst chooses first sleeping group
func (gm *GM) chooseFirst() int32 {
	for i := 1; i < len(gm.counts); i++ {
		if atomic.LoadInt32(&gm.counts[i]) > 0 {
			return int32(i)
		}
	}
	return empty
}
