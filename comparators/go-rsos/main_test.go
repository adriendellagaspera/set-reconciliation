package main

import (
	"context"
	"reflect"
	"testing"
)

func TestExactDifferences(t *testing.T) {
	for _,shape := range []string{"deletion-scattered","deletion-clustered","updated-scattered","right-only-scattered"} {
		for _,layout := range []string{"ordered","hashed"} {
			for _,build := range []string{"bulk","incremental"} {
				t.Run(shape+"/"+layout+"/"+build,func(t *testing.T){
					c:=Case{Records:128,ChangedKeys:7,Shape:shape,Layout:layout,Build:build,SplitFactor:16,Threshold:16}
					out,err:=runCase(context.Background(),c)
					if err!=nil {t.Fatal(err)}
					wantL,wantR:=[]Element{},[]Element{}
					for i:=uint64(0);i<c.Records;i++ {
						if !changed(i,c) {continue}
						switch shape {
						case "deletion-scattered","deletion-clustered":wantL=append(wantL,Element{i,0})
						case "right-only-scattered":wantR=append(wantR,Element{i,0})
						case "updated-scattered":wantL=append(wantL,Element{i,0});wantR=append(wantR,Element{i,1})
						}
					}
					if !reflect.DeepEqual(out.LeftOnly,wantL) || !reflect.DeepEqual(out.RightOnly,wantR) {
						t.Fatalf("wrong diff: got %v / %v, want %v / %v",out.LeftOnly,out.RightOnly,wantL,wantR)
					}
				})
			}
		}
	}
}

func TestEmptyAndIdentical(t *testing.T) {
	for _,n := range []uint64{0,64} {
		out,err:=runCase(context.Background(),Case{Records:n,ChangedKeys:0,Shape:"deletion-scattered",Layout:"hashed",Build:"bulk",SplitFactor:16,Threshold:128})
		if err!=nil {t.Fatal(err)}
		if len(out.LeftOnly)!=0 || len(out.RightOnly)!=0 {t.Fatalf("unexpected differences: %+v",out)}
	}
}

func TestRejectInvalid(t *testing.T) {
	c:=Case{Records:5,ChangedKeys:6,Shape:"deletion-scattered",Layout:"hashed",Build:"bulk",SplitFactor:16,Threshold:16}
	if err:=validate(c);err==nil {t.Fatal("must reject invalid workload")}
}
