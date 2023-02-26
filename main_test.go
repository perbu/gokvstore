package kv

import (
	"fmt"
	"os"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	header1 := jEncode(OpSet, 42, 2424)
	op, key, value, err := jDecode(header1)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpSet {
		t.Fatalf("expected OpSet, got %d", op)
	}
	if key != 42 {
		t.Fatalf("expected key 42, got %d", key)
	}
	if value != 2424 {
		t.Fatalf("expected value 2424, got %d", value)
	}
}

func deleteFiles(files ...string) error {
	for _, file := range files {
		err := os.Remove(file)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("deleting %s: %w", file, err)
		}
	}
	return nil
}

func TestBasic(t *testing.T) {
	err := deleteFiles("test.db", "test.wal")
	if err != nil {
		t.Fatal(err)
	}
	kv, err := New("test.db", "test.wal")
	if err != nil {
		t.Fatal(err)
	}

	err = kv.Set("foo", 1)
	if err != nil {
		t.Fatal(err)
	}
	err = kv.Flush()
	if err != nil {
		t.Fatal(err)
	}
	err = kv.Close()
	if err != nil {
		t.Fatal(err)
	}

	kv2, err := New("test.db", "test.wal")
	val, ok, err := kv2.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("foo not found")
	}
	if val != 1 {
		t.Fatalf("expected 1, got %v", val)
	}
	err = kv2.Coalesce()
	if err != nil {
		t.Fatal(err)
	}
	err = kv2.Close()
	if err != nil {
		t.Fatal(err)
	}
	kv3, err := New("test.db", "test.wal")
	val, ok, err = kv3.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("foo not found")
	}
	if val != 1 {
		t.Fatalf("expected 1, got %v", val)
	}
	err = kv3.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestNewKV(t *testing.T) {
	kv, err := New("test.db", "test.wal")
	if err != nil {
		t.Fatal("creating kv:", err)
	}
	if kv == nil {
		t.Error("kv is nil")
	}
	kv.Set("foo", 1)
	foo, ok, err := kv.Get("foo")
	if err != nil {
		t.Fatal("getting foo:", err)
	}
	if !ok {
		t.Error("foo not found")
	}
	if foo != 1 {
		t.Errorf("foo is %v, want 1", foo)
	}
	// check that foo is a int:
	_, ok = foo.(int)
	if !ok {
		t.Errorf("foo is %T, want int", foo)
	}

	kv.Set("bar", "baz")
	bar, ok, err := kv.Get("bar")
	if err != nil {
		t.Fatal("getting bar:", err)
	}
	if !ok {
		t.Error("bar not found")
	}
	if bar != "baz" {
		t.Errorf("bar is %v, want baz", bar)
	}
	// check that bar is a string:
	_, ok = bar.(string)
	if !ok {
		t.Errorf("bar is %T, want string", bar)
	}
	// get a non-existent key:
	val, ok, err := kv.Get("non-existent")
	if err != nil {
		t.Fatal("getting non-existent:", err)
	}
	if ok {
		t.Errorf("ok is %v, want false", val)
	}
	err = kv.Flush()
	if err != nil {
		t.Fatal("flushing kv:", err)
	}
}

func TestSaveLoad(t *testing.T) {
	kv, err := New("test.db", "test.wal")
	if err != nil {
		t.Fatal(err)
	}
	err = kv.Set("foo", 1)
	if err != nil {
		t.Fatal(err)
	}
	err = kv.Set("bar", "baz")
	if err != nil {
		t.Fatal(err)
	}

	err = kv.Close()
	if err != nil {
		t.Error(err)
	}
	fmt.Println("== saved kv to disk ==")

}

func Test_Journaling(t *testing.T) {
	kv, err := New("test.db", "test.wal")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		kv.Set(fmt.Sprintf("key-%d", i), i)
	}
	err = kv.Flush()
	if err != nil {
		t.Fatal(err)
	}
	err = kv.Close()
	if err != nil {
		t.Fatal(err)
	}
}
