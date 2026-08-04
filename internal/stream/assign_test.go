package stream

import (
	"reflect"
	"testing"
)

func TestAssignShardsDisjoint(t *testing.T) {
	shards := []string{"shard-2", "shard-0", "shard-1", "shard-3"}
	a := assignShards(shards, "fraudguard-worker-0", 2)
	b := assignShards(shards, "fraudguard-worker-1", 2)

	if len(a)+len(b) != len(shards) {
		t.Fatalf("expected full cover, got %v + %v", a, b)
	}
	seen := map[string]bool{}
	for _, id := range append(append([]string{}, a...), b...) {
		if seen[id] {
			t.Fatalf("duplicate assignment for %s", id)
		}
		seen[id] = true
	}
	wantA := []string{"shard-0", "shard-2"}
	wantB := []string{"shard-1", "shard-3"}
	if !reflect.DeepEqual(a, wantA) {
		t.Fatalf("worker-0: want %v got %v", wantA, a)
	}
	if !reflect.DeepEqual(b, wantB) {
		t.Fatalf("worker-1: want %v got %v", wantB, b)
	}
}

func TestAssignShardsSingleReplicaOwnsAll(t *testing.T) {
	shards := []string{"b", "a"}
	got := assignShards(shards, "fraudguard-worker-0", 1)
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v got %v", want, got)
	}
}

func TestWorkerIndex(t *testing.T) {
	cases := map[string]int{
		"fraudguard-worker-0": 0,
		"fraudguard-worker-3": 3,
		"local":               0,
		"worker":              0,
	}
	for name, want := range cases {
		if got := workerIndex(name); got != want {
			t.Fatalf("%s: want %d got %d", name, want, got)
		}
	}
}
