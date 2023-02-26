package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
)

type journal struct {
	fh        io.WriteCloser // the underlying file handle, only used for closing.
	bufWriter *bufio.Writer
	name      string
}

var (
	ErrJournalCorrupt = errors.New("journal is corrupt")
)

// newJournal initiates a journal.
// if the journal already exists, it will be opened and re-played.
func newJournal(filename string, kv *kvMap) (journal, error) {

	// check if the journal exists:
	_, err := os.Stat(filename)
	if err == nil {
		err := play(filename, kv)
		if err != nil {
			return journal{}, fmt.Errorf("play: %w", err)
		}
	}
	// journal replayed. Now create a new journal:
	fh, err := os.Create(filename)
	if err != nil {
		return journal{}, fmt.Errorf("create: %w", err)
	}
	log.Printf("journal '%s' created", filename)
	buffed := bufio.NewWriter(fh)
	j := journal{
		name:      filename,
		fh:        fh,
		bufWriter: buffed,
	}
	return j, nil
}

func (j *journal) truncate() error {
	err := j.close()
	if err != nil {
		return fmt.Errorf("close: %w", err)
	}

	fh, err := os.Create(j.name)
	if err != nil {
		return fmt.Errorf("truncate: create: %w", err)
	}
	buffed := bufio.NewWriter(fh)
	j.fh = fh
	j.bufWriter = buffed
	return nil
}

func (j *journal) delete() error {
	err := os.Remove(j.name)
	if err != nil {
		return fmt.Errorf("delete, remove: %w", err)
	}
	return nil
}

func (j *journal) close() error {
	err := j.flush()
	if err != nil {
		return fmt.Errorf("flush journal: %w", err)
	}
	err = j.fh.Close()
	if err != nil {
		return fmt.Errorf("close journal: %w", err)
	}
	return nil
}

func (j *journal) flush() error {
	err := j.bufWriter.Flush()
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

type Tx struct {
	Key   string
	Value any
}

// jEncode will encode the operation and return a byte slice ready to be written to the journal.
func jEncode(op Op, length uint32, crc uint32) []byte {
	buf := make([]byte, 9)
	buf[0] = byte(op)
	binary.BigEndian.PutUint32(buf[1:5], length)
	binary.BigEndian.PutUint32(buf[5:9], crc)
	return buf
}

func jDecode(buf []byte) (Op, uint32, uint32, error) {
	if len(buf) != 9 {
		return 0, 0, 0, fmt.Errorf("expected 9 bytes, got %d", len(buf))
	}
	op := Op(buf[0])
	length := binary.BigEndian.Uint32(buf[1:5])
	crc := binary.BigEndian.Uint32(buf[5:9])
	return op, length, crc, nil
}

// play will open the journal and replay all the transactions in it, updating the supplied kvMap
func play(filename string, m *kvMap) error {
	fh, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open journal '%s': %w", filename, err)
	}
	defer fh.Close()
	for {
		// first read the header, 9 bytes:
		header := make([]byte, 9)
		n, err := fh.Read(header)
		if err != nil {
			if err == io.EOF {
				log.Println("EOF on journal")
				break
			}
			return fmt.Errorf("read header: %w", err)
		}
		if n != 9 {
			return fmt.Errorf("read header: expected 9 bytes, got %d", n)
		}
		// read the operation from the first byte:
		op, buflen, checksum, err := jDecode(header)
		if err != nil {
			return fmt.Errorf("decode header: %w", err)
		}
		// read the buffer:
		buf := make([]byte, buflen)
		n, err = fh.Read(buf)
		if err != nil {
			return fmt.Errorf("read buffer: %w", err)
		}
		if n != int(buflen) {
			return fmt.Errorf("read buffer: expected %d bytes, got %d", buflen, n)
		}
		// calculate the checksum of the buffer:
		crc := crc32.ChecksumIEEE(buf)
		if crc != checksum {
			return ErrJournalCorrupt
		}
		// decode the buffer:
		dec := gob.NewDecoder(bytes.NewReader(buf))
		var tx Tx
		err = dec.Decode(&tx)
		if err != nil {
			return fmt.Errorf("decode tx: %w", err)
		}
		// apply the transaction:
		switch op {
		case OpSet:
			(*m)[tx.Key] = tx.Value
		case OpUnset:
			delete(*m, tx.Key)
		}
	}
	return nil
}

func (j *journal) log(op Op, key string, value any) error {
	tx := Tx{
		Key:   key,
		Value: value,
	}
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(tx)
	if err != nil {
		return fmt.Errorf("encode tx: %w", err)
	}
	buflen := uint32(buf.Len())
	// calculate the checksum of the buffer:
	checksum := crc32.ChecksumIEEE(buf.Bytes())

	// make a 9 byte buffer to hold the header.
	header := jEncode(op, buflen, checksum)
	// write the header:
	n, err := j.bufWriter.Write(header)
	if err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if n != 9 {
		return fmt.Errorf("header write: expected 9 bytes, got %d", n)
	}

	// write the buffer:
	n, err = j.bufWriter.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("write buffer: %w", err)
	}
	if n != int(buflen) {
		return fmt.Errorf("buffer write: expected %d bytes, got %d", buflen, n)
	}
	return nil
}
