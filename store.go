package chord

type Store interface {
	Set(key uint64, value []byte) error
	Get(key uint64) ([]byte, error)
}

type MemoryStore map[uint64][]byte

func (s MemoryStore) Set(key uint64, value []byte) error {
	s[key] = value;
	return nil
}

func (s MemoryStore) Get(key uint64) ([]byte, error) {
	return s[key], nil
}