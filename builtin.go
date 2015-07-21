package main

import (
	"regexp"
)

func grok(in <-chan map[string]string, field, pattern string) <-chan map[string]string {
	r := regexp.MustCompile(pattern)

	return mapOver(in, func(m map[string]string) map[string]string {
		matches := r.FindStringSubmatch(m[field])
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

func merge(in <-chan map[string]string, fields map[string]string) <-chan map[string]string {
	return mapOver(in, func(m map[string]string) map[string]string {
		for k, v := range fields {
			m[k] = v
		}

		return m
	})
}

func choice(in <-chan map[string]string, field string, options map[string]func(<-chan map[string]string) <-chan map[string]string) <-chan map[string]string {
	c := make(chan map[string]string)

	kmap := make(map[string]int)
	iarr := make([]chan map[string]string, len(options)+1)
	oarr := make([]<-chan map[string]string, len(options)+1)

	iarr[0] = c
	oarr[0] = c

	for k, fn := range options {
		kmap[k] = len(oarr)
		iarr = append(iarr, make(chan map[string]string))
		oarr = append(oarr, fn(iarr[len(oarr)]))
	}

	go func() {
		for m := range in {
			iarr[kmap[m[field]]] <- m
		}
	}()

	return fanIn(oarr...)
}

func parseKV(in <-chan map[string]string) <-chan map[string]string {
	return mapOver(in, func(m map[string]string) map[string]string {
		return m
	})
}
