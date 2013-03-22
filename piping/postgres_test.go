package piping

import (
	"l2met/store"
	"testing"
	"time"
)

//Tests low level postgres interaction by sending a single bucket to it
//using the same code that the outlet uses.
//TODO: add verificaion
func TestPgSendBucket(t *testing.T) {
	err := store.WriteBucketToPostgres(GenerateBucket())

	if err != nil {
		t.Errorf("postgres returned error %v", err)
		t.Fail()
	}
}

//Tests sending a batch of items to postgres
//TODO: add verification beyond postgres saying trust me it worked
func TestPgSendBatch(t *testing.T) {
	bslice := NewBucketSlice(4)
	count := store.WriteSliceToPostgres(bslice, 4)

	if count != 4 {
		t.Errorf("wrong number of items written to pg")
	}

}

//Tests setting up a fairly realistic group of compenents to be used
//with the PostgresOutlet
func TestPgSendMulti(t *testing.T) {
	testSource := NewRandomSource(8)
	pgOutlet := NewPostgresOutlet(testSource.GetOutput(), 4, 4)
	pgOutlet2 := NewPostgresOutlet(testSource.GetOutput(), 4, 4)
	testSource.Start()
	pgOutlet.Start()
	pgOutlet2.Start()
	for pgOutlet.GetMetrics()["commits"]+pgOutlet2.GetMetrics()["commits"] < 8 {
		time.Sleep(time.Second)
	}
	pgOutlet2.Stop()
	pgOutlet.Stop()
	if pgOutlet.GetMetrics()["commits"]+pgOutlet2.GetMetrics()["commits"] < 8 {
		t.FailNow()
	}
	if len(testSource.GetOutput()) != 0 {
		t.FailNow()
	}
}
