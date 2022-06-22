package chord

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

type DHTServer struct {
	node  *LocalNode
	store Store
}

// NewDHTServer binds a node to a given store.
func NewDHTServer(node *LocalNode, store Store) (*DHTServer, error) {
	node.OnPredecessor(func(predecessor Node) {
		// delete all the keys up to the predecessor's id because they now own it.
		if err := store.Constrain(predecessor.ID(), node.ID()); err != nil {
			// TODO: error handling
			log.Printf("error when constraining %v", err)
		}
	})
	if node.successors[0] != node {
		// make this node a replicant of the successor.
		resp, err := http.Get(fmt.Sprintf("http://%s/store", node.successors[0].Host()))
		if err != nil {
			return nil, err
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		var data map[uint64][]byte
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, err
		}
		for key, value := range data {
			if err := store.Set(key, bytes.NewReader(value)); err != nil {
				return nil, err
			}
		}
	}
	return &DHTServer{node: node, store: store}, nil
}

func (s *DHTServer) Get(key uint64) (io.Reader, error) {
	node, err := s.node.FindSuccessor(key)
	if err != nil {
		return nil, err
	}
	if node.ID() == s.node.ID() {
		return s.store.Get(key)
	}
	resp, err := http.Get(fmt.Sprintf("http://%s/store?key=%x", node.Host(), key))
	if err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		return nil, io.ErrUnexpectedEOF
	}
	return resp.Body, nil
}

func (s *DHTServer) Set(key uint64, value io.Reader) error {
	node, err := s.node.FindSuccessor(key)
	if err != nil {
		return err
	}
	if node.ID() == s.node.ID() {
		return s.store.Set(key, value)
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/store?key=%x", node.Host(), key), "application/octet-stream", value)
	if err != nil {
		return err
	} else if resp.StatusCode != 200 {
		return io.ErrShortWrite
	}
	return err
}

func (s *DHTServer) HTTPServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/node", s.node.HTTPHandlerFunc())
	mux.Handle("/store", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case "GET":
			key := req.URL.Query().Get("key")
			if key == "" {
				body, err := json.Marshal(s.store.All())
				if err != nil {
					w.WriteHeader(500)
					return
				}
				if _, err := w.Write(body); err != nil {
					w.WriteHeader(500)
					return
				}
			} else {
				intkey, err := strconv.ParseUint(key, 16, 64)
				if err != nil {
					log.Printf("error %v", err)
					w.WriteHeader(500)
					return
				}
				value, err := s.Get(intkey)
				if err != nil {
					log.Printf("error %v", err)
					w.WriteHeader(500)
					return
				}
				if _, err := io.Copy(w, value); err != nil {
					log.Printf("error %v", err)
					w.WriteHeader(500)
					return
				}
			}

		case "POST":
			key := req.URL.Query().Get("key")
			if key == "" {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					w.WriteHeader(400)
					return
				}
				var data map[uint64][]byte
				if err := json.Unmarshal(body, &data); err != nil {
					w.WriteHeader(400)
					return
				}
				for key, value := range data {
					if err := s.store.Set(key, bytes.NewReader(value)); err != nil {
						w.WriteHeader(500)
						return
					}
				}
				w.WriteHeader(200)
			} else {
				intkey, err := strconv.ParseUint(key, 16, 64)
				if err != nil {
					log.Printf("error %v", err)
					w.WriteHeader(500)
					return
				}
				if err := s.Set(intkey, req.Body); err != nil {
					log.Printf("error %v", err)
					w.WriteHeader(500)
					return
				}
				w.WriteHeader(200)
			}
		default:
			w.WriteHeader(400)
		}
	}))
	return mux
}

func (s *DHTServer) String() string {
	return fmt.Sprintf("--- dht ---\n%v\n--- store ---\n%v", s.node, s.store)
}

func (s *DHTServer) Close() error {
	// send the data to the predecessor.
	body, err := json.Marshal(s.store.All())
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/store", s.node.predecessor.Host()), "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}
	return nil
}