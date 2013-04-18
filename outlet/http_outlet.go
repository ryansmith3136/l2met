package outlet

import (
	"errors"
	"l2met/bucket"
	"l2met/store"
	"l2met/utils"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type HttpOutlet struct {
	Store store.Store
	Query url.Values
}

func (h *HttpOutlet) ServeReadBucket(w http.ResponseWriter, r *http.Request) {
	// need to extract: token, source, name, time
	// https://l2met:token@l2met.net/buckets/:name
	user, pass, err := utils.ParseAuth(r)
	if err != nil {
		http.Error(w, "Inavalid Authentication", 401)
		return
	}

	// Shortcut so we can quickly access query params.
	h.Query = r.URL.Query()

	//We need to build the identity of a bucket before we can fetch
	//it from the store. Thus, the following attrs are parsed and held
	//for the bucket.Id.
	src := h.Query.Get("source")
	name := h.Query.Get("name")
	if len(name) == 0 {
		http.Error(w, "Invalid Request. Name is required.", 400)
		return
	}
	res, err := h.parseAssertion("tol", 60)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}
	resolution := time.Second * time.Duration(res)
	units := h.Query.Get("units")
	if len(units) == 0 {
		units = bucket.DefaultUnit
	}

	//The limit and offset are shortcuts to work with the time
	//field on the bucket. This makes it easy for the client to not have
	//to worry about keeping correct time.
	limit, err := h.parseAssertion("limit", 1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}
	//The offset is handy because you may not want to take the most recent
	//bucket. For instance, the current minute will not have a complete view
	//of the data; however, the last minute should.
	offset, err := h.parseAssertion("offset", 1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}

	//The API supports the ability to assert what metrics should be.
	//If the value of the assertion is negative, the assertion can
	//be skipped. By default, the value is negative.
	countAssertion, err := h.parseAssertion("count", -1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}
	meanAssertion, err := h.parseAssertion("mean", -1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}
	sumAssertion, err := h.parseAssertion("sum", -1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}
	//The tolerance is a way to work with assertions that would like to use
	//less than or greater than operators.
	tol, err := h.parseAssertion("tol", 0)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}

	//Build one bucket.Id to share across all the buckets that we fetch
	//with respect to the limit.
	//We will set the time in proceeding for loop.
	id := &bucket.Id{
		User:       user,
		Pass:       pass,
		Name:       name,
		Source:     src,
		Resolution: resolution,
		Units:      units,
	}
	resBucket := &bucket.Bucket{Id: id}
	anchorTime := time.Now()
	for i := 0; i < limit; i++ {
		x := time.Duration((i+offset)*-1) * resolution
		id.Time = utils.RoundTime(anchorTime.Add(x), resolution)
		b := &bucket.Bucket{Id: id}
		//Fetch the bucket from our store.
		//This will fill in the vals.
		h.Store.Get(b)
		//We are only returning 1 bucket from the API. The
		//bucket will contain an aggregate view of the data based on limit.
		resBucket.Add(b)
	}

	//If any of the assertion values are -1 then they were not
	//defined in the request query params. Thus, we only do our assertions
	//if the assertion parameter is > 0.
	if countAssertion > 0 {
		if math.Abs(float64(resBucket.Count()-countAssertion)) > float64(tol) {
			http.Error(w, "Count assertion failed.", 404)
			return
		}
	}
	if meanAssertion > 0 {
		if math.Abs(float64(resBucket.Mean()-float64(meanAssertion))) > float64(tol) {
			http.Error(w, "Mean assertion failed.", 404)
			return
		}
	}
	if sumAssertion > 0 {
		if math.Abs(float64(resBucket.Sum()-float64(sumAssertion))) > float64(tol) {
			http.Error(w, "Sum assertion failed.", 404)
			return
		}
	}

	//Assuming there was not a failed assertion, we can return the result
	//bucket which may contain an aggregate of other buckets via bucket.Add()
	utils.WriteJson(w, 200, resBucket)
}

// Returns -1 when a value was specified in data.
func (h *HttpOutlet) parseAssertion(name string, defaultVal int) (int, error) {
	tmpVal := h.Query.Get(name)
	if len(tmpVal) == 0 {
		return defaultVal, nil
	}
	val, err := strconv.Atoi(tmpVal)
	if err != nil {
		return -1, errors.New("Assertion must be a positive integer.")
	}
	if val < 0 {
		return -1, errors.New("Assertion must be a positive integer.")
	}
	return val, nil
}
