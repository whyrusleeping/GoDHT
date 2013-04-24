package main

import (
	"net"
	"encoding/binary"
)


const (
	MLockReq = byte(iota)
	MUpdateVal
	MLockRelease
	MRemindVal
)

type LockInfo struct {
	Holder string
	time int
}

type entry struct {
	val string
	lock *LockInfo
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

func (d *DHT) HandleConnection(c net.Conn) {
	flagByte := []byte{0}
	for {
		c.Read(flagByte)
		switch flagByte[0] {
			case MLockReq:
				buffer := make([]byte, 8)
				c.Read(buffer)
				key, _ := binary.Uvarint(buffer)
				c.Read(buffer)
				timestamp, _ := binary.Uvarint(buffer)
				go d.DoLockRequest(key, timestamp)
			case MLockRelease:
			case MUpdateVal:
			case MRemindVal:
		}
	}
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
	
	for {
		c, err := list.AcceptTCP()
		go d.HandleConnection(c)
	}
}


func (d *DHT) DoLockRequest(key uint64, ts uint64) {

}

//attempts to get a lock on the given key string across the network, returns success
func (d *DHT) GetLock(onkey uint64) bool {
	//Send request for lock to each node that this node is connected to
	for _, c := range d.peerCons {
		
	}
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

func (d *DHT) GetVal(key int) string {
	//Wait for any locks on this entry to resolve
	for d.arr[key].lock == true {
		time.Sleep(time.Millisecond)
	}
	return d.arr[key].v
}

func (d *DHT) SetValue(key int, val string) {
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
