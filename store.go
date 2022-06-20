package chord

type Store interface {
	Set(key uint64, value []byte) error
	Get(key uint64) ([]byte, error)
	All() map[uint64][]byte
	Constrain(a, b uint64) error
}

type MemoryStore map[uint64][]byte

func (s MemoryStore) Set(key uint64, value []byte) error {
	s[key] = value;
	return nil
}

func (s MemoryStore) Get(key uint64) ([]byte, error) {
	return s[key], nil
}

func (s MemoryStore) All() map[uint64][]byte {
	return s
}

func (s MemoryStore) Constrain(a, b uint64) error {
	for k := range s {
		if between(k, a, b) {
			delete(s, k)
		}
	}
	return nil
}