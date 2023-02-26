package kv

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Op uint8

const (
	OpSet Op = iota + 1
	OpUnset
)

type kvMap map[string]any
type KV struct {
	memory    kvMap
	fileName  string
	journal   journal
	mu        sync.Mutex
	lastFlush time.Time
	ready     atomic.Bool
}

var (
	ErrNotReady = errors.New("kv is not ready")
)

// New will create a new KV store. The dump file will be empty, the journal will be where all
// the changes are stored until they are coalesced into the dump file though a call to Coalesce.
func New(dbName, walName string) (*KV, error) {

	memory := make(kvMap)
	// check if the dump file exists, if it exists the load the content into memory.
	_, err := os.Stat(dbName)
	switch err {
	case nil:
		memory, err = loadFromGob(dbName)
		if err != nil {
			return nil, fmt.Errorf("loading from existing gob: %w", err)
		}
	default:
		err = createEmptyGob(dbName)
		if err != nil {
			return nil, fmt.Errorf("creating empty gob: %w", err)
		}
	}
	journal, err := newJournal(walName, &memory)
	if err != nil {
		return nil, fmt.Errorf("creating journal: %w", err)
	}
	kv := &KV{
		fileName: dbName,
		memory:   memory,
		journal:  journal,
	}
	kv.ready.Store(true)
	return kv, nil
}

func loadFromGob(dbName string) (kvMap, error) {
	var memory kvMap
	fh, err := os.Open(dbName)
	if err != nil {
		return nil, fmt.Errorf("opening file '%s': %w", dbName, err)
	}
	defer fh.Close()
	dec := gob.NewDecoder(fh)
	err = dec.Decode(&memory)
	if err != nil {
		return nil, fmt.Errorf("decoding map: %w", err)
	}
	return memory, nil
}

func createEmptyGob(dbName string) error {
	fh, err := os.Create(dbName)
	if err != nil {
		return fmt.Errorf("creating file '%s': %w", dbName, err)
	}
	defer fh.Close()
	enc := gob.NewEncoder(fh)
	err = enc.Encode(make(kvMap))
	if err != nil {
		return fmt.Errorf("createEmptyGob: encoding map: %w", err)
	}
	return nil
}

// dump will dump the kv.memory map to disk.
// It assumes kv is locked.
// journal should be deleted before or after this, while lock is kept.
func (kv *KV) dump() error {
	if !kv.ready.Load() {
		return ErrNotReady
	}

	fh, err := os.Create(kv.fileName)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	// use gob to encode the map to disk:
	enc := gob.NewEncoder(fh)
	err = enc.Encode(kv.memory)
	if err != nil {
		return fmt.Errorf("encoding map: %w", err)
	}
	err = fh.Close()
	if err != nil {
		return fmt.Errorf("closing file: %w", err)
	}
	return nil
}

// Coalesce will coalesce the journal into the dump file.
// It will delete the journal after it is done and create a new one.
func (kv *KV) Coalesce() error {
	if !kv.ready.Load() {
		return ErrNotReady
	}
	start := time.Now()
	defer log.Printf("save took %v\n", time.Since(start))
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// persist the kv.memory map to disk
	err := kv.dump()
	if err != nil {
		return fmt.Errorf("dumping memory: %w", err)
	}

	err = kv.journal.truncate()
	if err != nil {
		return fmt.Errorf("truncating journal: %w", err)
	}
	return nil
}

func (kv *KV) Flush() error {
	if !kv.ready.Load() {
		return ErrNotReady
	}
	err := kv.journal.bufWriter.Flush()
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastFlush = time.Now()
	return nil
}

// Close closes the journal, doesn't save a new dump.
func (kv *KV) Close() error {
	if !kv.ready.Load() {
		return ErrNotReady
	}
	start := time.Now()
	defer func() {
		log.Printf("close took %v\n", time.Since(start))
	}()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	err := kv.journal.close()
	if err != nil {
		return fmt.Errorf("closing journal: %w", err)
	}
	kv.ready.Store(false)
	return nil
}

func (kv *KV) Set(key string, value any) error {
	if kv.ready.Load() == false {
		return ErrNotReady
	}
	kv.mu.Lock()
	kv.memory[key] = value
	kv.mu.Unlock()
	// persist the key to disk:
	err := kv.journal.log(OpSet, key, value)
	if err != nil {
		log.Printf("error persisting key '%s': %v", key, err)
	}
	return nil
}

func (kv *KV) Unset(key string) (bool, error) {
	if kv.ready.Load() == false {
		return false, ErrNotReady
	}
	kv.mu.Lock()
	_, ok := kv.memory[key]
	// it doesn't exist in memory, so no need to log the deletion.
	if !ok {
		return true, nil
	}
	kv.mu.Unlock()
	// persist the deletion to disk:
	err := kv.journal.log(OpUnset, key, nil)
	if err != nil {
		return true, fmt.Errorf("journaling: %w", err)
	}
	return true, nil
}

func (kv *KV) Get(key string) (any, bool, error) {
	if kv.ready.Load() == false {
		return nil, false, ErrNotReady
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.memory == nil {
		kv.memory = make(kvMap)
	}
	val, ok := kv.memory[key]
	return val, ok, nil
}
