package piping

import (
	"fmt"
	"l2met/store"
	"math/rand"
	"os"
	"time"
)

var chars = "abcdefghijklmonpqrstuvwxyz"

//Generates a random string of a givin length
//Useful when generating buckets
func NewRandomString(length int) (r string) {
	if length < 1 {
		return
	}
	b := make([]byte, length)
	for i, _ := range b {
		b[i] = chars[rand.Intn(24)]
	}
	r = string(b)
	return r
}

//Generates a new UUID, though not complient with UUID2 standards
func NewUUID() string {
	f, _ := os.Open("/dev/urandom")
	b := make([]byte, 16)
	f.Read(b)
	f.Close()
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid
}

//Generates a slice of floats (again useful for generating buckets)
func NewRandomFloatSlice(length int) (r []float64) {
	if length < 1 {
		return
	}
	r = make([]float64, length)
	for idx, _ := range r {
		r[idx] = rand.Float64()
	}
	return r
}

//A source that generates a givin number of random buckets and exposes 
//a map containing all of the generated buckets which can be retrived using
//the key property of the bucket.
type RandomSource struct {
	sender   *SingleSender
	testList map[string]*store.Bucket
	control  chan bool
	count    int
}

//Generates a single random bucket that works with l2met.
func GenerateBucket() (b *store.Bucket) {
	b = &store.Bucket{
		Key: store.BKey{
			Token:  NewUUID(),
			Name:   NewRandomString(5),
			Source: NewRandomString(10),
			Time:   time.Now()},
		Vals: NewRandomFloatSlice(50)}
	return b
}

//Returns a new Random source that will generate a givin number of 
//Buckets
func NewRandomSource(count int) (t *RandomSource) {
	t = &RandomSource{
		sender:   NewSingleSender(),
		testList: make(map[string]*store.Bucket, 100),
		count:    count}
	return t
}

func (t *RandomSource) Start() {
	go t.runGenerateBuckets()
	go t.sender.Start()
}

func (t *RandomSource) Stop() {
	t.control <- true
	t.sender.Stop()
}

//Generates a slive of random buckets
func NewBucketSlice(count int) []*store.Bucket {
	buckets := make([]*store.Bucket, count)
	for i := 0; i < count; i = i + 1 {
		buckets[i] = GenerateBucket()
	}
	return buckets
}

func (t *RandomSource) runGenerateBuckets() {
	i := 0
	for {
		select {
		case <-t.control:
			return
		default:
			if i < t.count {
				b := GenerateBucket()
				t.PutBucket(b)
				i++
			} else {
				return
			}
		}
	}
}

func (t *RandomSource) GetOutput() chan *store.Bucket {
	return t.sender.GetOutput()
}

//Buts a bucket into the list of generated buckets and propagates it to the
//output channel of the random source.
func (t *RandomSource) PutBucket(b *store.Bucket) {
	t.testList[b.Key.Token] = b
	t.sender.GetSenderChannel() <- b
}
