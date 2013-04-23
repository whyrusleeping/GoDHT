package main

import (
  "net"
)
type entry struct {
	k, v string
	lock bool
}

type DHT struct { 
	arr []entry
	peers []string
	h Hash
}

type Hash interface {
	func hash(s string) int
}

type LameHash struct {}
func (L LameHash) hash(s string, m int) int64 {
	i := int64(0)
	for n, c := range s {
		i += (n * c + m)**2
	}
	return i
}

func NewDHT(size int) *DHT {
	return &DHT{make([]entry, size), make([]string, 128)}
}

func (d *DHT) GetVal(key string) string {
	n := d.h.hash(key, 0)
	for i := 1; d.arr[n].k != key && i < 256; i++ {
		if d.arr[n].k == "" {
			return "";
		}
		n = d.h.hash(key, i)
	}
	if d.arr[n].k != key {
		panic("Bad Hash Function!!")
	}
	//Wait for any locks on this entry to resolve
	for d.arr[n].lock == true {
		time.Sleep(time.Millisecond)
	}
	return d.arr[n].v
}

func (d *DHT) ConnectToPeer(addr string) {
	
}
