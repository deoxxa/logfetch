package main

import (
	"sync"
)

func fanIn(in ...<-chan map[string]string) <-chan map[string]string {
	r := make(chan map[string]string)

	var wg sync.WaitGroup
	for i := range in {
		wg.Add(1)
		go func(c <-chan map[string]string) {
			defer wg.Done()

			for m := range c {
				r <- m
			}
		}(in[i])
	}

	go func() {
		defer close(r)
		wg.Wait()
	}()

	return r
}

func fanOut(in <-chan map[string]string, out ...func(<-chan map[string]string) <-chan map[string]string) []<-chan map[string]string {
	r := make([]<-chan map[string]string, len(out))

	c := make([]chan map[string]string, len(out))
	for i, o := range out {
		c[i] = make(chan map[string]string)
		r[i] = o(c[i])
	}

	go func() {
		defer func() {
			for i := range c {
				close(c[i])
			}
		}()

		for m := range in {
			for i := range c {
				c[i] <- m
			}
		}
	}()

	return r
}

func fanThrough(in <-chan map[string]string, via ...func(<-chan map[string]string) <-chan map[string]string) <-chan map[string]string {
	return fanIn(fanOut(in, via...)...)
}
