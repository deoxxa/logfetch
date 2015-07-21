package logfetch

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type LogglyOptions struct {
	Host string
	Key  string
}

func SendToLoggly(in <-chan map[string]interface{}, options LogglyOptions) <-chan map[string]interface{} {
	o := make(chan map[string]interface{})

	go func() {
		defer close(o)

		for m := range in {
			d, err := json.Marshal(m)
			if err != nil {
				panic(err)
			}

			r, err := http.Post("http://"+options.Host, "application/json", bytes.NewReader(d))
			if err != nil || r.StatusCode != 200 {
				o <- m
			}
		}
	}()

	return o
}
