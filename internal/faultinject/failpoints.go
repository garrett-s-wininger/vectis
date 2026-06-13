package faultinject

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Point string

type Hook interface {
	Before(context.Context, Point) error
}

var ErrInjected = errors.New("fault injected")

type Script struct {
	mu       sync.Mutex
	hits     map[Point]int
	failures map[Point][]scriptedFailure
}

type scriptedFailure struct {
	hit int
	err error
}

func NewScript() *Script {
	return &Script{
		hits:     make(map[Point]int),
		failures: make(map[Point][]scriptedFailure),
	}
}

func (s *Script) FailNext(point Point, err error) {
	s.FailOn(point, s.Hits(point)+1, err)
}

func (s *Script) FailOn(point Point, hit int, err error) {
	if hit <= 0 {
		hit = 1
	}

	if err == nil {
		err = ErrInjected
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.failures[point] = append(s.failures[point], scriptedFailure{hit: hit, err: err})
}

func (s *Script) Before(ctx context.Context, point Point) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.hits[point]++
	hit := s.hits[point]
	failures := s.failures[point]
	for i, failure := range failures {
		if failure.hit != hit {
			continue
		}

		s.failures[point] = append(failures[:i], failures[i+1:]...)
		return fmt.Errorf("%s hit %d: %w", point, hit, failure.err)
	}

	return nil
}

func (s *Script) Hits(point Point) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.hits[point]
}
