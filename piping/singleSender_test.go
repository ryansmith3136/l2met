package piping

import (
	"l2met/store"
	"testing"
	"time"
)

func TestSingleSender(t *testing.T) {
	s := NewSingleSender()
	schan := s.GetSenderChannel()
	ichan := s.NewOutputChannel("test", 5)
	schan <- &store.Bucket{
		Key: store.BKey{Name: "test"}}

	go s.Start()
	time.Sleep(10)
	select {
	case <-ichan:
		return
	default:
		t.Errorf("value was not copied")
	}
}
