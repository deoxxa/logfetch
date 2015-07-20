package main

import (
	"fmt"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("logfetcher", "Log receiver, fetcher, and parser.")
)

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	fmt.Printf("# starting up\n")
	for _, f := range app.Model().Flags {
		fmt.Printf("# %s: %s\n", f.Name, f.Value)
	}

	collect()
}
