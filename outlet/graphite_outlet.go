package outlet

import (
	"fmt"
	"l2met/bucket"
	"net"
)

type GraphitePayload struct {
	Name string
	Val  float64
}

type GraphiteOutlet struct {
	Inbox    chan *bucket.Bucket
	Outbox   chan *GraphitePayload
	ApiToken string
	Reader   Reader
}

func (g *GraphiteOutlet) Start() {
	go g.Reader.Start(g.Inbox)
	go g.convert()
	go g.outlet()
}

func NewGraphiteOutlet() *GraphiteOutlet {
	g := new(GraphiteOutlet)
	g.Inbox = make(chan *bucket.Bucket, 1000)
	g.Outbox = make(chan *GraphitePayload, 1000)
	return g
}

func (g *GraphiteOutlet) convert() {
	for bucket := range g.Inbox {
		name := bucket.Id.Name
		if len(bucket.Id.Source) > 0 {
			name = bucket.Id.Source + "." + name
		}
		g.Outbox <- &GraphitePayload{name + ".min", bucket.Min()}
		g.Outbox <- &GraphitePayload{name + ".median", bucket.Median()}
		g.Outbox <- &GraphitePayload{name + ".perc95", bucket.P95()}
		g.Outbox <- &GraphitePayload{name + ".perc99", bucket.P99()}
		g.Outbox <- &GraphitePayload{name + ".max", bucket.Max()}
		g.Outbox <- &GraphitePayload{name + ".mean", bucket.Mean()}
		g.Outbox <- &GraphitePayload{name + ".sum", bucket.Sum()}
	}
}

func (g *GraphiteOutlet) outlet() {
	for payload := range g.Outbox {
		conn, err := net.Dial("udp", "carbon.hostedgraphite.com:2003")
		if err != nil {
			continue
		}
		fmt.Fprintf(conn, "%s.%s %f", g.ApiToken, payload.Name, payload.Val)
		conn.Close()
	}
}
