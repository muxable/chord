package main

import (
	"context"
	"flag"
	"math/rand"
	"net/http"
	"os"
	"os/signal"

	"github.com/muxable/chord"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5001", "the address to listen on")
	join := flag.String("join", "", "the address to join")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	local := chord.NewLocalNode(ctx, rand.Uint64(), *addr)

	if *join != "" {
		remote, err := chord.NewRemoteNode(*join)
		if err != nil {
			panic(err)
		}
		if err := local.Join(remote); err != nil {
			panic(err)
		}
	}

	dht := chord.NewDHTServer( local, &chord.MemoryStore{},)

	server := &http.Server{Addr: *addr, Handler: dht.HTTPServeMux()}

	go server.ListenAndServe()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	
	// stop accepting incoming requests
	server.Shutdown(context.Background())

	// forward data
	cancel()
}