package piping

import (
	"l2met/store"
	"l2met/utils"
	"time"
)

//The postgres outlet accepts a channel of incomming buckets and dumps them
//into postgres. If the bucket is already partially in postgres then it is 
//updated with the differnces that are presented in the incomming buckets
//
//The PostgresOutlet also batches the incomming buckets so that they can
//be flushed if the outlet needs to stop, and to prepare for batched queries
//to improve the performance of the postgres outlet
type PostgresOutlet struct {
	reciver   *SingleReciver
	control   chan bool
	metrics   map[string]int
	batch     []*store.Bucket
	delay     uint
	batchSize uint
	flush     chan bool
	batchPos  int
	ticker    chan bool
}

//Accepts and incomming channel, a batchsize (the point at which a batch is
//dumpped into Postgres, and the intended delay for flushing
//the bucket batch.
func NewPostgresOutlet(input chan *store.Bucket, batchSize uint, batchDelay uint) (p *PostgresOutlet) {
	p = &PostgresOutlet{
		reciver:   NewSingleReciver(input),
		control:   make(chan bool),
		batch:     make([]*store.Bucket, batchSize),
		delay:     batchDelay,
		batchSize: batchSize,
		flush:     make(chan bool),
		batchPos:  0,
		ticker:    make(chan bool)}
	p.initMetrics()
	return p
}

//Trying this out, need to have a uniform way to do monitoring internally,
//while stdout is great for and observer it is not so awesome for l2met
//internally
func (p *PostgresOutlet) initMetrics() {
	p.metrics = make(map[string]int)
	p.metrics["commits"] = 0
}

//Resets the internall metrics counters
//This is an alpha feature and may be removed
func (p *PostgresOutlet) RestartMetrics() {
	p.initMetrics()
}

//Starts the outlets compoents --
//The aggragator to postgres
//The reciver
//The batcher
func (p *PostgresOutlet) Start() {
	utils.MeasureI("postgres.outlet.start.count", 1)
	go p.runPutBuckets()
	go p.runBatcher()
	go p.reciver.Start()
}

//Stops everything, any metrics in memory *will not* be put in PG
func (p *PostgresOutlet) Stop() {
	p.reciver.Stop()
	p.control <- true
	utils.MeasureI("postgres.outlet.stop.count", 1)
}

//Get the metrics hash
func (p *PostgresOutlet) GetMetrics() map[string]int {
	return p.metrics
}

func (p *PostgresOutlet) runBatcher() {
	for {
		select {
		case <-p.control:
			//persist signal
			p.control <- true
			return
		case <-time.Tick(time.Duration(p.delay) * time.Second):
			utils.MeasureI("postgres.outlet.batch.time.tick", 1)
			p.flush <- true
		case <-p.tick():
			utils.MeasureI("postgres.outlet.batch.filled.tick", 1)
			p.flush <- true
		}
	}
}

func (p *PostgresOutlet) runPutBuckets() {
	for {
		select {
		case <-p.control:
			p.control <- true
			utils.MeasureI("postgres.outlet.control.signal", 1)
			return
		case next := <-p.reciver.input:
			p.AddToBatch(next)
			utils.MeasureI("postgres.outlet.batch.size", int64(p.batchPos))
		case <-p.flush:
			utils.MeasureI("postgres.outlet.batch.flush", 1)
			p.Flush()
		}
	}
}

func (p *PostgresOutlet) tick() chan bool {
	return p.ticker
}

//Flushes the metrics in memory out to pg
func (p *PostgresOutlet) Flush() {
	count := store.WriteSliceToPostgres(p.batch, p.batchPos)
	p.metrics["commits"] = count + p.metrics["commits"]
	if (p.batchPos) == count {
		p.batchPos = 0
	}
}

//Manually add a bucket to the internal batch 
//This is useful when you want to inject metrics 
//or don't want to use the input channel
func (p *PostgresOutlet) AddToBatch(bucket *store.Bucket) {
	p.batch[p.batchPos] = bucket
	p.batchPos++
	if uint(p.batchPos) > p.batchSize {
		p.ticker <- true
	}
}
