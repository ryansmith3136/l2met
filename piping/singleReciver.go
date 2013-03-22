package piping

import (
	"l2met/store"
)

//just a wrapper around a channel with a stop and start method
//this may be evidence that there is not abstraction needed for a reciver
//but it does not seem unreasonable to want to be able to recive from multiple
//channels at some point so I kept the abstraction
type SingleReciver struct {
	input chan *store.Bucket
}

func NewSingleReciver(input chan *store.Bucket) (s *SingleReciver) {
	s = &SingleReciver{
		input}
	return s
}

func (s *SingleReciver) Input() chan *store.Bucket {
	return s.input
}

func (s *SingleReciver) Start() {
}

func (s *SingleReciver) Stop() {
}
