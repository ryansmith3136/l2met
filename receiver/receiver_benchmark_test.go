package receiver

import (
	"l2met/store"
	"testing"
	"time"
)

func BenchmarkReceive(b *testing.B) {
	b.StopTimer()
	st := store.NewMemStore()
	recv := NewReceiver(1, 1, time.Second, st)
	recv.Start()
	defer recv.Stop()

	opts := make(map[string][]string)
	opts["user"] = []string{"u"}
	opts["password"] = []string{"p"}
	msg := []byte("94 <190>1 2013-03-27T20:02:24+00:00 hostname token shuttle - - measure.hello=99 measure.world=100")

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		recv.Receive(msg, opts)
		b.StopTimer()
		b.SetBytes(int64(len(msg)))
	}
}
