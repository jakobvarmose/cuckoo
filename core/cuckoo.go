package cuckoo

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	mrand "math/rand"
	"os"
)

const (
	headerSize = 256
	hashSize   = 12
	bucketSize = 16
)

type Cuckoo struct {
	f       *os.File
	slots   int
	buckets int
	shards  int
	seed    []byte
}

func CreateCuckoo(filename string, bucketBits int) (*Cuckoo, error) {
	slots := 8
	shards := 1
	buckets := 1 << uint(bucketBits)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(headerSize + int64(slots*buckets*shards)*bucketSize)
	if err != nil {
		f.Close()
		return nil, err
	}
	header := make([]byte, headerSize)
	copy(header, "IPFS pins\xff\xff\xff")
	binary.BigEndian.PutUint32(header[12:], 1 /*version*/)
	binary.BigEndian.PutUint32(header[16:], uint32(slots))
	binary.BigEndian.PutUint32(header[20:], uint32(buckets))
	binary.BigEndian.PutUint32(header[24:], uint32(shards))
	seed := make([]byte, 16)
	_, err = crand.Read(seed)
	if err != nil {
		f.Close()
		return nil, err
	}
	copy(header[32:], seed)
	f.WriteAt(header, 0)
	f.Close()
	return OpenCuckoo(filename)
}

func OpenCuckoo(filename string) (*Cuckoo, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	header := make([]byte, 256)
	_, err = f.ReadAt(header, 0)
	if err != nil {
		f.Close()
		return nil, err
	}
	if string(header[:12]) != "IPFS pins\xff\xff\xff" {
		f.Close()
		return nil, errors.New("Not a pin file")
	}
	if binary.BigEndian.Uint32(header[12:]) != 1 {
		f.Close()
		return nil, errors.New("Version not supported")
	}
	c := new(Cuckoo)
	c.f = f
	c.slots = int(binary.BigEndian.Uint32(header[16:]))
	c.buckets = int(binary.BigEndian.Uint32(header[20:]))
	c.shards = int(binary.BigEndian.Uint32(header[24:]))
	c.seed = make([]byte, 16)
	copy(c.seed, header[32:])
	return c, nil
}

func (c Cuckoo) Close() error {
	return c.f.Close()
}

func (c Cuckoo) Sync() error {
	return c.f.Sync()
}

func (c Cuckoo) sum(in string) (int, []byte) {
	h := sha256.New()
	h.Write(c.seed)
	h.Write([]byte(in))
	t := h.Sum(nil)
	return int(binary.BigEndian.Uint32(t[0:4]) % uint32(c.buckets)), t[4 : 4+hashSize]
}

func (c Cuckoo) other(key int, sum []byte) int {
	return key ^ int(binary.BigEndian.Uint32(sum[0:4])%uint32(c.buckets))
}

func (c Cuckoo) Get(in string) (uint32, error) {
	keys := []int{0, 0}
	var sum1 []byte
	keys[0], sum1 = c.sum(in)
	keys[1] = c.other(keys[0], sum1)
	itemss := [][]byte{nil, nil}
	var err error
	itemss[0], err = c.getItems(keys[0])
	if err != nil {
		return 0, err
	}
	itemss[1], err = c.getItems(keys[1])
	if err != nil {
		return 0, err
	}
	for i := 0; i < c.shards*c.slots; i++ {
		for j := 0; j < 2; j++ {
			item := itemss[j][bucketSize*i : bucketSize*i+bucketSize]
			if bytes.Compare(item[:hashSize], sum1[:hashSize]) == 0 {
				return binary.BigEndian.Uint32(item[hashSize:]), nil
			}
		}
	}
	return 0, nil
}

func (c *Cuckoo) Increment(in string) error {
	keys := []int{0, 0}
	var sum1 []byte
	keys[0], sum1 = c.sum(in)
	keys[1] = c.other(keys[0], sum1)
	itemss := [][]byte{nil, nil}
	var err error
	itemss[0], err = c.getItems(keys[0])
	if err != nil {
		return err
	}
	itemss[1], err = c.getItems(keys[1])
	if err != nil {
		return err
	}
	// Check if the key is already stored
	for i := 0; i < c.shards*c.slots; i++ {
		for j := 0; j < 2; j++ {
			item := itemss[j][bucketSize*i : bucketSize*i+bucketSize]
			if bytes.Compare(item[:hashSize], sum1[:hashSize]) == 0 {
				// Increment the key and store it back
				binary.BigEndian.PutUint32(item[hashSize:], binary.BigEndian.Uint32(item[hashSize:])+1)
				c.putItem(keys[j], i, item)
				return nil
			}
		}
	}
	// Find an empty slot to store the key
	for i := 0; i < c.shards*c.slots; i++ {
		for j := 0; j < 2; j++ {
			item := itemss[j][bucketSize*i : bucketSize*i+bucketSize]
			if binary.BigEndian.Uint32(item[hashSize:]) == 0 {
				// Store the key in this empty slot
				copy(item[:hashSize], sum1[:hashSize])
				binary.BigEndian.PutUint32(item[hashSize:], 1)
				return c.putItem(keys[j], i, item)
			}
		}
	}
	// Move items around to make room for the key
	item := make([]byte, bucketSize)
	copy(item[:hashSize], sum1[:hashSize])
	binary.BigEndian.PutUint32(item[hashSize:], 1)
	key := keys[0]

	for x := 0; x < 100; x++ {
		otherKey := c.other(key, item[:hashSize])
		otherItems, err := c.getItems(otherKey)
		if err != nil {
			return err
		}
		for k := 0; k < c.shards*c.slots; k++ {
			if binary.BigEndian.Uint32(otherItems[bucketSize*k : bucketSize*k+bucketSize][hashSize:]) == 0 {
				return c.putItem(otherKey, k, item)
			}
		}
		otherI := mrand.Intn(c.shards * c.slots)
		otherItem := otherItems[bucketSize*otherI : bucketSize*otherI+bucketSize]
		err = c.putItem(otherKey, otherI, item)
		if err != nil {
			return err
		}
		key = otherKey
		item = otherItem
	}
	// Insertion failed so let's allocate more memory
	c.shards++
	err = c.f.Truncate(256 + int64(c.shards*c.buckets*c.slots)*bucketSize)
	if err != nil {
		return err
	}
	shards := make([]byte, 4)
	binary.BigEndian.PutUint32(shards, uint32(c.shards))
	c.f.WriteAt(shards, 24)
	err = c.putItem(key, c.slots*(c.shards-1), item)
	if err != nil {
		return err
	}
	return nil
}

func (c Cuckoo) Decrement(in string) error {
	keys := []int{0, 0}
	var sum1 []byte
	keys[0], sum1 = c.sum(in)
	keys[1] = c.other(keys[0], sum1)
	itemss := [][]byte{nil, nil}
	var err error
	itemss[0], err = c.getItems(keys[0])
	if err != nil {
		return err
	}
	itemss[1], err = c.getItems(keys[1])
	if err != nil {
		return err
	}
	for i := 0; i < c.shards*c.slots; i++ {
		for j := 0; j < 2; j++ {
			item := itemss[j][bucketSize*i : bucketSize*i+bucketSize]
			if bytes.Compare(item[:hashSize], sum1[:hashSize]) == 0 {
				value := binary.BigEndian.Uint32(item[hashSize:]) - 1
				binary.BigEndian.PutUint32(item[hashSize:], value)
				if value == 0 {
					copy(item[:hashSize], make([]byte, hashSize))
				}
				c.putItem(keys[j], i, item)
				return nil
			}
		}
	}
	return errors.New("Not found")
}

func (c Cuckoo) putItem(b int, index int, data []byte) error {
	shard := index / c.slots
	slot := index % c.slots
	_, err := c.f.WriteAt(data, 256+int64(shard*c.buckets*c.slots+b*c.slots+slot)*bucketSize)
	return err
}

func (c Cuckoo) getItems(b int) ([]byte, error) {
	items := make([]byte, int64(c.shards*c.slots)*bucketSize)
	for shard := 0; shard < c.shards; shard++ {
		_, err := c.f.ReadAt(items[shard*c.slots*bucketSize:shard*c.slots*bucketSize+c.slots*bucketSize], 256+int64(shard*c.buckets*c.slots+b*c.slots)*bucketSize)
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}
