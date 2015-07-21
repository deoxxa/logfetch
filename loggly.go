package main

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type logglyOptions struct {
	host string
	key  string
}

func sendToLoggly(in <-chan map[string]interface{}, options logglyOptions) <-chan map[string]interface{} {
	o := make(chan map[string]interface{})

	go func() {
		defer close(o)

		for m := range in {
			d, err := json.Marshal(m)
			if err != nil {
				panic(err)
			}

			r, err := http.Post("http://"+options.host, "application/json", bytes.NewReader(d))
			if err != nil || r.StatusCode != 200 {
				o <- m
			}
		}
	}()

	return o
}
