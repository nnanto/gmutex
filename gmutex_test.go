package gmutex

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupMutex_Simple(t *testing.T) {
	m := New(2)
	for i := 0; i < 100; i++ {
		m.Lock(1)
		m.Unlock(1)
	}
	for i := 0; i < 100; i++ {
		m.Lock(2)
		m.Unlock(2)
	}
	isClean(t, m)
}

/**
Test parallel Locks
*/

func parallelLock(lock, unlock func(), clocked, cunlock, cdone chan struct{}) {
	lock()
	clocked <- struct{}{}
	<-cunlock
	unlock()
	cdone <- struct{}{}
}

func doTestParallel(lock, unlock func(), count, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	clocked := make(chan struct{})
	cunlock := make(chan struct{})
	cdone := make(chan struct{})
	for i := 0; i < count; i++ {
		go parallelLock(lock, unlock, clocked, cunlock, cdone)
	}
	for i := 0; i < count; i++ {
		<-clocked
	}
	for i := 0; i < count; i++ {
		cunlock <- struct{}{}
	}
	for i := 0; i < count; i++ {
		<-cdone
	}

}

func TestParallelGroup(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	m := New(2)
	params := [][]int{{1, 4}, {3, 4}, {4, 2}}
	lock := func() {
		m.Lock(1)
	}
	unlock := func() {
		m.Unlock(1)
	}
	for _, cgo := range params {
		doTestParallel(lock, unlock, cgo[0], cgo[1])
	}
	isClean(t, m)
}

/**

 */

func groupMutexRunner(m *GM, group int32, numIterations int, activity *int32, cdone chan struct{}) {
	for i := 0; i < numIterations; i++ {
		if i%3 == 0 {
			for i := 0; i < 100; i++ {
				i *= 2
				i /= 2
			}
		}
		m.Lock(group)
		if group&1 == 0 {
			atomic.AddInt32(activity, 1)
		} else {
			atomic.AddInt32(activity, -1)
		}
		m.Unlock(group)
		if i%7 == 0 {
			for i := 0; i < 100; i++ {
				i *= 2
				i /= 2
			}
		}
	}
	cdone <- struct{}{}
}

func hammerGroupMutex(t *testing.T, gomaxprocs, numRWs, numIterations int, strategy Strategy) {
	t.Helper()
	runtime.GOMAXPROCS(gomaxprocs)
	var activity int32
	msize := 4
	m := NewWithStrategy(msize, strategy)
	cdone := make(chan struct{})

	for i := 0; i < numRWs; i++ {
		for i := 0; i < msize; i++ {
			go groupMutexRunner(m, int32(i+1), numIterations, &activity, cdone)
		}
	}
	for i := 0; i < msize*numRWs; i++ {
		<-cdone
	}
	if activity != 0 {
		t.Fatalf("Activity is expected to be 0, got: %v", activity)
	}
	isClean(t, m)
}

func TestHammerGroupMutex(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 1000
	if testing.Short() {
		n = 5
	}
	strategies := []Strategy{ChooseMax, ChooseRandom, ChooseFirst}
	for _, strategy := range strategies {
		hammerGroupMutex(t, 1, 1, n, strategy)
		hammerGroupMutex(t, 1, 3, n, strategy)
		hammerGroupMutex(t, 1, 10, n, strategy)
		hammerGroupMutex(t, 4, 1, n, strategy)
		hammerGroupMutex(t, 4, 3, n, strategy)
		hammerGroupMutex(t, 4, 10, n, strategy)
		hammerGroupMutex(t, 10, 1, n, strategy)
		hammerGroupMutex(t, 10, 3, n, strategy)
		hammerGroupMutex(t, 10, 10, n, strategy)
		hammerGroupMutex(t, 10, 5, n, strategy)
	}

}

func TestThorHammerGroupMutex(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 1000
	if testing.Short() {
		n = 100
	}
	strategies := []Strategy{ChooseMax, ChooseRandom, ChooseFirst}
	for _, strategy := range strategies {
		hammerGroupMutex(t, 10, 100, n, strategy)
	}
}

func TestGroupAsRW(t *testing.T) {
	m := New(2)
	wg := sync.WaitGroup{}
	action := int32(0)
	for i := 0; i < 2000; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			m.Lock(1)
			defer m.Unlock(1)
			time.Sleep(300*time.Millisecond)
			atomic.AddInt32(&action, 1)
		}()

		go func() {
			defer wg.Done()
			m.Lock(2)
			defer m.Unlock(2)
			time.Sleep(10*time.Millisecond)
			atomic.AddInt32(&action, -1)
		}()
	}
	wg.Wait()
	if action != 0 {
		t.Fatalf("Expected 0, got %v\n", action)
	}
}

func BenchmarkGroupMutexUncontended(b *testing.B) {
	type PaddedRWMutex struct {
		*GM
		pad [32]uint32
	}
	b.RunParallel(func(pb *testing.PB) {
		var rwm PaddedRWMutex
		rwm.GM = New(2)
		for pb.Next() {
			rwm.Lock(2)
			rwm.Unlock(2)
			rwm.Lock(2)
			rwm.Unlock(2)
			rwm.Lock(1)
			rwm.Lock(1)
			rwm.Unlock(1)
			rwm.Unlock(1)
		}
	})
}

func benchmarkGroupMutex(b *testing.B, localWork, writeRatio, size int) {
	rwm := New(size)
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			r := foo % writeRatio
			if r == 0 {
				rwm.Lock(1)
				rwm.Unlock(1)
			} else if size > 2 && r < writeRatio/4 {
				rwm.Lock(3)
				rwm.Unlock(3)
			} else if size > 3 && r < writeRatio/2 {
				rwm.Lock(4)
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				rwm.Unlock(4)
			} else {
				rwm.Lock(2)
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				rwm.Unlock(2)
			}
		}
		_ = foo

	})
	isClean(b, rwm)
}

func BenchmarkGroupMutex(b *testing.B) {

	localWork := []int{0, 100}
	groups := []int{2, 3, 4}
	writeRatios := []int{0, 100}
	for _, gs := range groups {
		for _, lw := range localWork {
			for _, wr := range writeRatios {
				b.Run(fmt.Sprintf("%v work %v groups %v write_ratio", lw, gs, wr),
					func(b *testing.B) {
						benchmarkGroupMutex(b, lw, 100, gs)
					})
			}

		}
	}
}

func isClean(tb testing.TB, rwm *GM) {
	if rwm.brokerState != empty {
		tb.Fatalf("Lock shouldn't be held at the end: %v\n", rwm.brokerState)
	}
	for _, count := range rwm.counts {
		if count != 0 {
			tb.Fatalf("All groups must be reset to zero")
		}
	}
}
