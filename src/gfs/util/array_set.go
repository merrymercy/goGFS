package util

import (
	"math/rand"
	"sync"
)

// ArraySet is a set implemented using array. I suppose it'll provide better
// performance than golang builtin map when the set is really really small.
// It is thread-safe since a mutex is used.
type ArraySet struct {
	arr  []interface{}
	lock sync.RWMutex
}

// Add adds an element to the set.
func (s *ArraySet) Add(element interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, v := range s.arr {
		if v == element {
			return
		}
	}
	s.arr = append(s.arr, element)
}

// Delete delete an element in the set.
func (s *ArraySet) Delete(element interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, v := range s.arr {
		if v == element {
			s.arr = append(s.arr[:i], s.arr[i+1:]...)
			break
		}
	}
}

// Size returns the size of the set.
func (s *ArraySet) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.arr)
}

// RandomPick picks a random element from the set.
func (s *ArraySet) RandomPick() interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.arr) == 0 {
		panic("ArraySet.RandomPick : No element to pick")
	}
	return s.arr[rand.Int()%len(s.arr)]
}

// GetAll returns all elements of the set.
func (s *ArraySet) GetAll() []interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return append([]interface{}(nil), s.arr...)
}

// GetAllAndClear returns all elements of the set.
func (s *ArraySet) GetAllAndClear() []interface{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	old := s.arr
	s.arr = make([]interface{}, 0)
	return old
}
