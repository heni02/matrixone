// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package malloc

import "unsafe"

type ShardedAllocator []Allocator

func NewShardedAllocator(numShards int, newShard func() Allocator) ShardedAllocator {
	var ret ShardedAllocator
	for i := 0; i < numShards; i++ {
		ret = append(ret, newShard())
	}
	return ret
}

var _ Allocator = ShardedAllocator{}

func (s ShardedAllocator) Allocate(size uint64) (unsafe.Pointer, Deallocator, error) {
	pid := runtime_procPin()
	runtime_procUnpin()
	return s[pid%len(s)].Allocate(size)
}