package main

import (
  "net"
)

type Hash interface {
	func hash(s string) int
}

type LameHash struct {}
func (L LameHash) hash(s string, m, size int) int64 {
	i := int64(0)
	for n, c := range s {
		i = (i + (n * c + m)**2) % size
	}
	return i
}

type entry struct {
	k, v string
	lock bool
}

type DHT struct { 
	ID string //Some value universally unique to this exact object (hash of IP, MAC, program execution time?)
	arr []entry
	peers []string
	peerCons []net.Conn
	h Hash
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

func (d *DHT) SetValue(key, val string) {
	//First, request a lock on this entry across the network
	//Set the value across the network
	//And then release the lock
}

func (d *DHT) ConnectToPeer(addr string) {
	//Make connection to node at given address
	//Merge entries in local hash tables
	//In the event of collisions between the hash tables, 
	//	do a standard collision procedure and propogate the changes to both networks
}
