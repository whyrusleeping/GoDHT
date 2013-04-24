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

const (
	MLockReq = iota,
	MSendVal
)

/*


*/

type entry struct {
	k, v string
	lock bool
	lockID string
}

type DHT struct {
	ID string //Some value universally unique to this exact object (hash of IP, MAC, program execution time?)
	arr []*entry
	peerCons []net.Conn
	numCons int
	h Hash
}

func NewDHT(size int) *DHT {
	dht := new(DHT)
	dht.ID = "SELF"
	dht.arr = make([]*entry, size)
	dht.peerCons = make([]net.Conn, 128)
	dht.h = LameHash{}
	return dht
}

func (d *DHT) Listen() {
	addr, err := net.ResolveTCPAddr("tcp",":8282")
	if err != nil {
		panic(err)
	}

	list, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		panic(err)
	}
	
	
}

//attempts to get a lock on the given key string across the network, returns success
func (d *DHT) GetLock(onkey string) bool {
	//Send request for lock to each node that this node is connected to
	//On each of those nodes, if the lock is available, take it, set the lockID to this nodes ID
	//send out lock requests to each node that they are connected to
	//If a node receives a lock request and the lock is already taken:
	//	check if the lock ID is this requesting node, if it is, return YES
	//	Otherwise, [resolve locking conflict]
	//Once all nodes return YES, return YES
	//If the nodes dont return YES in a certain time, return NO
}

func (d *DHT) ReleaseLock(onkey string) {

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
	if d.GetLock(key) {
		//Set the value across the network

		//And then release the lock
		d.ReleaseLock(key)
	}
}

func (d *DHT) ConnectToPeer(addr string) {
	nAddr := net.ResolveTCPAddr("tcp", addr)
	c, err := net.DialTCP("tcp", nil, nAddr)
	if err != nil {
		log.Printf("Failed to connect to %s.\n", addr)
		return
	}
	d.peerCons[d.numCons] = c
	d.numCons++
	//Do initial handshake
	//request hash table entries

}
