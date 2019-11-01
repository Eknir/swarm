// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package chunk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/sctx"
)

var TagUidFunc = rand.Uint32

type persistFn func(key string, v interface{}) error

// Tags hold tag information indexed by a unique random uint32
type Tags struct {
	mtx         sync.RWMutex
	tags        map[uint32]*Tag
	dirty       bool      // indicates whether the persisted tags are dirty
	persistFunc persistFn // a pluggable function to persist tags
}

// NewTags creates a tags object
// the supplied persistFn is used for persistence on
// shutdowns and on checkpoint persistence (on delete and on add)
func NewTags(fn persistFn) *Tags {
	return &Tags{
		tags:        make(map[uint32]*Tag),
		persistFunc: fn,
	}
}

// Create creates a new tag, stores it by the name and returns it
// it returns an error if the tag with this name already exists
func (ts *Tags) Create(s string, total int64, anon bool) (*Tag, error) {
	ts.mtx.Lock()
	defer ts.mtx.Unlock()
	t := NewTag(TagUidFunc(), s, total, anon)

	if _, loaded := ts.tags[t.Uid]; loaded {
		return nil, errExists
	}
	ts.tags[t.Uid] = t

	err := ts.Persist()
	if err != nil {
		log.Error("had an error while persisting tags on delete", "err", err)
	}

	return t, nil
}

// All returns all existing tags in Tags' sync.Map
// Note that tags are returned in no particular order
func (ts *Tags) All() (t []*Tag) {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	for _, v := range ts.tags {
		t = append(t, v)
	}

	return t
}

// Get returns the underlying tag for the uid or an error if not found
func (ts *Tags) Get(uid uint32) (*Tag, error) {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	t, ok := ts.tags[uid]
	if !ok {
		return nil, errTagNotFound
	}
	return t, nil
}

// GetByAddress returns the latest underlying tag for the address or an error if not found
func (ts *Tags) GetByAddress(address Address) (t *Tag, err error) {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	var lastTime time.Time
	for _, value := range ts.tags {
		rcvdTag := value
		if bytes.Equal(rcvdTag.Address, address) && rcvdTag.StartedAt.After(lastTime) {
			t = rcvdTag
			lastTime = rcvdTag.StartedAt
		}
	}

	if t == nil {
		return nil, errTagNotFound
	}
	return t, nil
}

// GetFromContext gets a tag from the tag uid stored in the context
func (ts *Tags) GetFromContext(ctx context.Context) (*Tag, error) {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	uid := sctx.GetTag(ctx)
	t, ok := ts.tags[uid]
	if !ok {
		return nil, errTagNotFound
	}
	return t, nil
}

// Range exposes sync.Map's iterator
func (ts *Tags) Range(fn func(k, v interface{}) bool) {
	for k, v := range ts.tags {
		cont := fn(k, v)
		if !cont {
			return
		}
	}
}

func (ts *Tags) Delete(k uint32) {
	ts.mtx.Lock()
	defer ts.mtx.Unlock()

	delete(ts.tags, k)
	err := ts.Persist()
	if err != nil {
		log.Error("had an error while persisting tags on delete", "err", err)
	}
}

// Persist calls the persistFunc with the current tags map
// the caller is expected to hold the tags.mtx under write lock
func (ts *Tags) Persist() error {
	return ts.persistFunc("tags", ts)
}

func (ts *Tags) MarshalJSON() (out []byte, err error) {
	m := make(map[string]*Tag)
	ts.Range(func(k, v interface{}) bool {
		key := fmt.Sprintf("%d", k)
		val := v.(*Tag)

		// don't persist tags which were already done
		if !val.Done(StateSynced) {
			m[key] = val
		}
		return true
	})
	return json.Marshal(m)
}

func (ts *Tags) UnmarshalJSON(value []byte) error {
	m := make(map[string]*Tag)
	err := json.Unmarshal(value, &m)
	if err != nil {
		return err
	}
	for k, v := range m {
		key, err := strconv.ParseUint(k, 10, 32)
		if err != nil {
			return err
		}

		// prevent a condition where a chunk was sent before shutdown
		// and the node was turned off before the receipt was received
		v.Sent = v.Synced

		ts.tags[uint32(key)] = v
	}

	return err
}
