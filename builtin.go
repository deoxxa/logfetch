package main

import (
	"regexp"
	"strconv"
	"time"
)

func grok(in <-chan map[string]interface{}, field, pattern string) <-chan map[string]interface{} {
	r := regexp.MustCompile(pattern)

	return mapOver(in, func(m map[string]interface{}) map[string]interface{} {
		f, ok := m[field]
		if !ok {
			return m
		}

		s, ok := f.(string)
		if !ok {
			return m
		}

		matches := r.FindStringSubmatch(s)
		if matches != nil {
			for i, k := range r.SubexpNames() {
				if i == 0 {
					continue
				}

				m[k] = matches[i]
			}
		}

		return m
	})
}

func merge(in <-chan map[string]interface{}, fields map[string]interface{}) <-chan map[string]interface{} {
	return mapOver(in, func(m map[string]interface{}) map[string]interface{} {
		for k, v := range fields {
			m[k] = v
		}

		return m
	})
}

func choice(in <-chan map[string]interface{}, field string, options map[string]func(<-chan map[string]interface{}) <-chan map[string]interface{}) <-chan map[string]interface{} {
	c := make(chan map[string]interface{})

	kmap := make(map[string]int)
	iarr := make([]chan map[string]interface{}, len(options)+1)
	oarr := make([]<-chan map[string]interface{}, len(options)+1)

	iarr[0] = c
	oarr[0] = c

	for k, fn := range options {
		kmap[k] = len(oarr)
		iarr = append(iarr, make(chan map[string]interface{}))
		oarr = append(oarr, fn(iarr[len(oarr)]))
	}

	go func() {
		for m := range in {
			f, ok := m[field]
			if !ok {
				c <- m
				continue
			}

			s, ok := f.(string)
			if !ok {
				c <- m
				continue
			}

			iarr[kmap[s]] <- m
		}
	}()

	return fanIn(oarr...)
}

func parseInt(in <-chan map[string]interface{}, keys []string) <-chan map[string]interface{} {
	return mapOver(in, func(m map[string]interface{}) map[string]interface{} {
		for _, k := range keys {
			f, ok := m[k]
			if !ok {
				continue
			}

			s, ok := f.(string)
			if !ok {
				continue
			}

			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				continue
			}

			m[k] = i
		}

		return m
	})
}

func parseNumber(in <-chan map[string]interface{}, keys []string) <-chan map[string]interface{} {
	return mapOver(in, func(m map[string]interface{}) map[string]interface{} {
		for _, k := range keys {
			f, ok := m[k]
			if !ok {
				continue
			}

			s, ok := f.(string)
			if !ok {
				continue
			}

			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				m[k] = n
				continue
			}

			if n, err := strconv.ParseFloat(s, 64); err == nil {
				m[k] = n
				continue
			}
		}

		return m
	})
}

func parseTime(in <-chan map[string]interface{}, key, layout string) <-chan map[string]interface{} {
	return mapOver(in, func(m map[string]interface{}) map[string]interface{} {
		f, ok := m[key]
		if !ok {
			return m
		}

		s, ok := f.(string)
		if !ok {
			return m
		}

		t, err := time.Parse(layout, s)
		if err != nil {
			return m
		}

		m[key] = t

		return m
	})
}

var timeFormats = []string{
	"Mon Jan _2 15:04:05 2006",
	"Mon Jan _2 15:04:05 MST 2006",
	"Mon Jan 02 15:04:05 -0700 2006",
	"02 Jan 06 15:04 MST",
	"02 Jan 06 15:04 -0700",
	"Monday, 02-Jan-06 15:04:05 MST",
	"Mon, 02 Jan 2006 15:04:05 MST",
	"Mon, 02 Jan 2006 15:04:05 -0700",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05.999999999Z07:00",
	"Jan _2 15:04:05",
	"Jan _2 15:04:05.000",
	"Jan _2 15:04:05.000000",
	"Jan _2 15:04:05.000000000",
	"_2/Jan/2006:15:04:05 -0700",
	"Jan 2, 2006 3:04:05 PM",
	"Jan 2 2006 15:04:05",
	"Jan 2 15:04:05 2006",
	"Jan 2 15:04:05 -0700",
	"2006-01-02 15:04:05,000 -0700",
	"2006-01-02 15:04:05 -0700",
	"2006-01-02 15:04:05-0700",
	"2006-01-02 15:04:05,000",
	"2006-01-02 15:04:05",
	"2006/01/02 15:04:05",
	"06-01-02 15:04:05,000 -0700",
	"06-01-02 15:04:05,000",
	"06-01-02 15:04:05",
	"06/01/02 15:04:05",
	"15:04:05,000",
	"1/2/2006 3:04:05 PM",
	"1/2/06 3:04:05.000 PM",
	"1/2/2006 15:04",
}

func guessTime(in <-chan map[string]interface{}, key string) <-chan map[string]interface{} {
	return mapOver(in, func(m map[string]interface{}) map[string]interface{} {
		f, ok := m[key]
		if !ok {
			return m
		}

		s, ok := f.(string)
		if !ok {
			return m
		}

		for _, layout := range timeFormats {
			t, err := time.Parse(layout, s)
			if err == nil {
				m[key] = t
				break
			}
		}

		return m
	})
}
