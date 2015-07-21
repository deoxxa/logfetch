package main

func filter(in <-chan map[string]interface{}, fn func(map[string]interface{}) bool) <-chan map[string]interface{} {
	r := make(chan map[string]interface{})

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

func mapOver(in <-chan map[string]interface{}, fn func(map[string]interface{}) map[string]interface{}) <-chan map[string]interface{} {
	r := make(chan map[string]interface{})

	go func() {
		defer close(r)

		for m := range in {
			r <- fn(m)
		}
	}()

	return r
}
