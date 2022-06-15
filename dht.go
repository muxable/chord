package chord

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

type DHTServer struct {
	node  *LocalNode
	store Store
}

// NewDHTServer binds a node to a given store.
func NewDHTServer(node *LocalNode, store Store) *DHTServer {
	return &DHTServer{node: node, store: store}
}

func (s *DHTServer) Get(key uint64) ([]byte, error) {
	node, err := s.node.FindSuccessor(key)
	if err != nil {
		return nil, err
	}
	resp, err := http.Get(fmt.Sprintf("http://%s/store/%x", node.Host(), key))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

func (s *DHTServer) Set(key uint64, value []byte) error {
	node, err := s.node.FindSuccessor(key)
	if err != nil {
		return err
	}
	_, err = http.Post(fmt.Sprintf("http://%s/store/%x", node.Host(), key), "application/octet-stream", bytes.NewReader(value))
	return err
}

func (s *DHTServer) HTTPServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/node", s.node.HTTPHandlerFunc())
	mux.Handle("/store", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		setValue := req.URL.Query().Get("value")
		key, err := strconv.ParseUint(req.URL.Query().Get("key"), 16, 64)
		if err != nil {
			w.WriteHeader(404)
			return
		}
		if setValue == "" {
			value, err := s.Get(key)
			if err != nil {
				w.WriteHeader(500)
				return
			}
			w.Write(value)
		} else {
			if err := s.Set(key, []byte(setValue)); err != nil {
				w.WriteHeader(500)
			} else {
				w.Write([]byte{})
			}
		}
	}))
	return mux
}
