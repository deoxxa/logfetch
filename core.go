package main

func filter(in <-chan map[string]string, fn func(map[string]string) bool) <-chan map[string]string {
	r := make(chan map[string]string)

	go func() {
		defer close(r)

		for m := range in {
			if fn(m) {
				r <- m
			}
		}
	}()

	return r
}

func mapOver(in <-chan map[string]string, fn func(map[string]string) map[string]string) <-chan map[string]string {
	r := make(chan map[string]string)

	go func() {
		defer close(r)

		for m := range in {
			r <- fn(m)
		}
	}()

	return r
}
