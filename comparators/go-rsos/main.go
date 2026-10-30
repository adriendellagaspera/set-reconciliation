// Command go-rsos-comparator benchmarks the upstream go-rsos implementation.
// It is an external research comparator, not a replication runtime.
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"time"

	rsos "github.com/phillipjad/go-rsos"
)

const upstream = "phillipjad/go-rsos@d4c1aee23b344dc9a61ada8ae3f5f849f5a841b2"

type Case struct {
	Records       uint64 `json:"records"`
	ChangedKeys   uint64 `json:"changed_keys"`
	Shape         string `json:"shape"`
	Layout        string `json:"layout"`
	Build         string `json:"build"`
	SplitFactor   int    `json:"split_factor"`
	Threshold     uint64 `json:"resolve_threshold"`
}

type Element struct {
	Key     uint64 `json:"key"`
	Version uint8  `json:"version"`
}

type IOStats struct {
	Gets          uint64 `json:"gets"`
	ReadBytes     uint64 `json:"read_bytes"`
	Puts          uint64 `json:"puts"`
	Deletes       uint64 `json:"deletes"`
	BatchCalls    uint64 `json:"batch_calls"`
	BatchOps      uint64 `json:"batch_ops"`
	WriteBytes    uint64 `json:"write_bytes"`
	DeleteRanges  uint64 `json:"delete_ranges"`
}

type Result struct {
	SchemaVersion int       `json:"schema_version"`
	Upstream      string    `json:"upstream"`
	Case          Case      `json:"case"`
	LeftOnly      []Element `json:"left_only"`
	RightOnly     []Element `json:"right_only"`
	BuildNs       int64     `json:"build_ns"`
	ReconcileNs   int64     `json:"reconcile_ns"`
	BuildIO       IOStats   `json:"build_io"`
	ReconcileIO   IOStats   `json:"reconcile_io"`
}

type countedStore struct {
	inner rsos.Store
	gets, readBytes, puts, deletes, batches, batchOps, writeBytes, ranges atomic.Uint64
}

func (s *countedStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	data, err := s.inner.Get(ctx, key)
	s.gets.Add(1)
	s.readBytes.Add(uint64(len(data)))
	return data, err
}

func (s *countedStore) Put(ctx context.Context, key, value []byte) error {
	err := s.inner.Put(ctx, key, value)
	if err == nil {
		s.puts.Add(1)
		s.writeBytes.Add(uint64(len(key) + len(value)))
	}
	return err
}

func (s *countedStore) Delete(ctx context.Context, key []byte) error {
	err := s.inner.Delete(ctx, key)
	if err == nil { s.deletes.Add(1) }
	return err
}

func (s *countedStore) WriteBatch(ctx context.Context, ops []rsos.Op) error {
	err := s.inner.WriteBatch(ctx, ops)
	if err != nil { return err }
	s.batches.Add(1)
	s.batchOps.Add(uint64(len(ops)))
	for _, op := range ops {
		s.writeBytes.Add(uint64(len(op.Key) + len(op.Value)))
	}
	return nil
}

func (s *countedStore) DeleteRange(ctx context.Context, start, end []byte) error {
	err := s.inner.DeleteRange(ctx, start, end)
	if err == nil { s.ranges.Add(1) }
	return err
}

func (s *countedStore) stats() IOStats {
	return IOStats{
		Gets:s.gets.Load(), ReadBytes:s.readBytes.Load(), Puts:s.puts.Load(),
		Deletes:s.deletes.Load(), BatchCalls:s.batches.Load(), BatchOps:s.batchOps.Load(),
		WriteBytes:s.writeBytes.Load(), DeleteRanges:s.ranges.Load(),
	}
}

func sum(a, b IOStats) IOStats {
	return IOStats{
		Gets:a.Gets+b.Gets, ReadBytes:a.ReadBytes+b.ReadBytes, Puts:a.Puts+b.Puts,
		Deletes:a.Deletes+b.Deletes, BatchCalls:a.BatchCalls+b.BatchCalls,
		BatchOps:a.BatchOps+b.BatchOps, WriteBytes:a.WriteBytes+b.WriteBytes,
		DeleteRanges:a.DeleteRanges+b.DeleteRanges,
	}
}

func delta(after, before IOStats) IOStats {
	return IOStats{
		Gets:after.Gets-before.Gets, ReadBytes:after.ReadBytes-before.ReadBytes,
		Puts:after.Puts-before.Puts, Deletes:after.Deletes-before.Deletes,
		BatchCalls:after.BatchCalls-before.BatchCalls, BatchOps:after.BatchOps-before.BatchOps,
		WriteBytes:after.WriteBytes-before.WriteBytes, DeleteRanges:after.DeleteRanges-before.DeleteRanges,
	}
}

func validate(c Case) error {
	if c.Records > 10_000_000 { return errors.New("records exceeds ten million") }
	if c.ChangedKeys > c.Records { return errors.New("changed_keys exceeds records") }
	switch c.Shape {
	case "deletion-scattered", "deletion-clustered", "updated-scattered", "right-only-scattered":
	default: return fmt.Errorf("unsupported shape %q", c.Shape)
	}
	if c.Layout != "ordered" && c.Layout != "hashed" { return fmt.Errorf("unsupported layout %q", c.Layout) }
	if c.Build != "bulk" && c.Build != "incremental" { return fmt.Errorf("unsupported build %q", c.Build) }
	if c.SplitFactor < 2 || c.SplitFactor > 64 { return errors.New("split_factor must be 2..64") }
	if c.Threshold < 1 || c.Threshold > 100_000 { return errors.New("resolve_threshold must be 1..100000") }
	if c.ChangedKeys == c.Records && c.Shape != "deletion-clustered" && c.ChangedKeys != 0 {
		return errors.New("scattered layouts need at least one unchanged key")
	}
	return nil
}

// splitmix64 is a permutation of uint64, used exclusively to vary physical ordering.
// Both language runners implement these exact operations. This is not a security hash.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func makeKey(index uint64, layout string) rsos.Key {
	var key rsos.Key
	if layout == "hashed" {
		binary.BigEndian.PutUint64(key[:8], splitmix64(index ^ 0x6a09e667f3bcc909))
		binary.BigEndian.PutUint64(key[8:], splitmix64(index ^ 0xbb67ae8584caa73b))
	} else {
		binary.BigEndian.PutUint64(key[8:], index)
	}
	return key
}

func changed(index uint64, c Case) bool {
	if c.ChangedKeys == 0 { return false }
	if c.Shape == "deletion-clustered" {
		start := (c.Records - c.ChangedKeys)/2
		return index >= start && index < start+c.ChangedKeys
	}
	for i:=uint64(1); i<=c.ChangedKeys; i++ {
		if index == (c.Records/(c.ChangedKeys+1))*i { return true }
	}
	return false
}

func value(index uint64, version uint8) uint64 {
	n := index * 2654435761
	if version != 0 { return ^n }
	return n
}

func mutation(index uint64, version uint8, layout string) rsos.Mutation {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value(index, version))
	return rsos.Mutation{ID:makeKey(index,layout), Ref:"item", Hash:raw[:]}
}

func sortMutations(muts []rsos.Mutation) {
	sort.Slice(muts,func(i,j int) bool {
		a,b:=muts[i].ID,muts[j].ID
		for k:=range a { if a[k]!=b[k] { return a[k]<b[k] } }
		return false
	})
}

func populate(ctx context.Context, forest *rsos.Forest, muts []rsos.Mutation, mode string) error {
	if mode=="bulk" {
		sortMutations(muts)
		return forest.Build(ctx,rsos.SliceStream(muts))
	}
	const batch=256
	for i:=0; i<len(muts); i+=batch {
		end:=i+batch
		if end>len(muts) {end=len(muts)}
		if err:=forest.ApplyMutations(ctx,muts[i:end]); err!=nil {return err}
	}
	return nil
}

func decode(entries []rsos.Entry, expected map[rsos.Key]Element) ([]Element,error) {
	out:=make([]Element,0,len(entries))
	for _,entry:=range entries {
		element,ok:=expected[entry.ID]
		if !ok {return nil,fmt.Errorf("unexpected diff key %x",entry.ID)}
		dg:=mutation(element.Key,element.Version,"ordered") // digest depends on ref and raw value, not ordering key
		want:=rsos.LeafDigest(dg.Ref,dg.Hash)
		if entry.Digest!=hex.EncodeToString(want[:]) {return nil,fmt.Errorf("digest mismatch for %d",element.Key)}
		out=append(out,element)
	}
	sort.Slice(out,func(i,j int)bool {return out[i].Key<out[j].Key})
	return out,nil
}

func runCase(ctx context.Context,c Case)(Result,error) {
	if err:=validate(c);err!=nil {return Result{},err}
	ls,rs:=&countedStore{inner:rsos.NewMemStore()},&countedStore{inner:rsos.NewMemStore()}
	left,right:=rsos.New(ls),rsos.New(rs)
	lm,rm:=make([]rsos.Mutation,0,c.Records),make([]rsos.Mutation,0,c.Records)
	lindex,rindex:=make(map[rsos.Key]Element),make(map[rsos.Key]Element)
	for i:=uint64(0);i<c.Records;i++ {
		k:=makeKey(i,c.Layout)
		leftPresent,rightPresent:=true,true
		leftVersion,rightVersion:=uint8(0),uint8(0)
		if changed(i,c) {
			switch c.Shape {
			case "deletion-scattered","deletion-clustered": rightPresent=false
			case "updated-scattered": rightVersion=1
			case "right-only-scattered": leftPresent=false
			}
		}
		if leftPresent {lm=append(lm,mutation(i,leftVersion,c.Layout));lindex[k]=Element{i,leftVersion}}
		if rightPresent {rm=append(rm,mutation(i,rightVersion,c.Layout));rindex[k]=Element{i,rightVersion}}
	}
	started:=time.Now()
	if err:=populate(ctx,left,lm,c.Build);err!=nil {return Result{},fmt.Errorf("build left: %w",err)}
	if err:=populate(ctx,right,rm,c.Build);err!=nil {return Result{},fmt.Errorf("build right: %w",err)}
	buildNs:=time.Since(started).Nanoseconds()
	before:=sum(ls.stats(),rs.stats())
	started=time.Now()
	diff,err:=rsos.Reconcile(ctx,left,right,rsos.WithSplitFactor(c.SplitFactor),rsos.WithResolveThreshold(c.Threshold))
	reconcileNs:=time.Since(started).Nanoseconds()
	if err!=nil {return Result{},fmt.Errorf("reconcile: %w",err)}
	lo,err:=decode(diff.LocalOnly,lindex)
	if err!=nil {return Result{},err}
	ro,err:=decode(diff.RemoteOnly,rindex)
	if err!=nil {return Result{},err}
	return Result{SchemaVersion:1,Upstream:upstream,Case:c,
		LeftOnly:lo,RightOnly:ro,BuildNs:buildNs,ReconcileNs:reconcileNs,
		BuildIO:before,ReconcileIO:delta(sum(ls.stats(),rs.stats()),before)},nil
}

func main(){
	dec:=json.NewDecoder(io.LimitReader(os.Stdin,4096))
	dec.DisallowUnknownFields()
	var c Case
	if err:=dec.Decode(&c);err!=nil {fmt.Fprintln(os.Stderr,err);os.Exit(1)}
	var extra any
	if err:=dec.Decode(&extra);err!=io.EOF {fmt.Fprintln(os.Stderr,"expected exactly one JSON case");os.Exit(1)}
	result,err:=runCase(context.Background(),c)
	if err!=nil {fmt.Fprintln(os.Stderr,err);os.Exit(1)}
	if err:=json.NewEncoder(os.Stdout).Encode(result);err!=nil {fmt.Fprintln(os.Stderr,err);os.Exit(1)}
}
