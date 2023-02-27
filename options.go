package kv

import "time"

type KvOption func(*KV)

// WithSyncInterval will set the interval between syncs.
// If the interval is 0, the sync will be disabled.
func WithSyncInterval(d time.Duration) KvOption {
	return func(kv *KV) {
		kv.syncInterval = d
	}
}

// WithSyncEvery will set the sync to happen after every operation.
// This will override the sync interval.
func WithSyncEvery() KvOption {
	return func(kv *KV) {
		kv.syncEvery = true
	}
}
