package main

import (
	"net"
	"encoding/binary"
	"crypto/sha1"
	"os"
	"crypto/rand"
	"fmt"
	"time"
)


type LockInfo struct {
	Holder string
	time uint64
}

type entry struct {
	val string
	lock *LockInfo
}

type DHT struct {
	ID []byte //Some value universally unique to this exact object (hash of IP, MAC, program execution time?)
	arr []*entry
	peerCons []net.Conn
	numCons int
	sendChan chan []byte
	lockReqs map[uint64]chan bool
	remReqs map[uint64]chan string
}

func GetUID() []byte {
	h := sha1.New()
	host, _ := os.Hostname()
	wd, _ := os.Getwd()
	salt := make([]byte, 64)
	rand.Read(salt)
	h.Write([]byte(host))
	h.Write([]byte(wd))
	h.Write(salt)
	ID := h.Sum(nil)
	return ID
}

func NewDHT(size int) *DHT {
	dht := new(DHT)
	dht.ID = GetUID()
	dht.arr = make([]*entry, size)
	dht.peerCons = make([]net.Conn, 128)
	go dht.Listen()
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
				c.Read(buffer)
				idlen, _ := binary.Uvarint(buffer)
				id := make([]byte, idlen)
				c.Read(id)
				go d.DoLockRequest(c, string(id), key, timestamp)
			case MLockRelease:
			case MLockRespYes, MLockRespNo:
				buffer := make([]byte, 8)
				c.Read(buffer)
				key, _ := binary.Uvarint(buffer)
				c.Read(buffer)
				ts, _  := binary.Uvarint(buffer)
				c.Read(buffer)
				idl, _ := binary.Uvarint(buffer)
				id := make([]byte, idl)
				c.Read(id)
				go d.DoLockResponse(key, ts, id, flagByte[0])

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

	list, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}

	for {
		c, _ := list.AcceptTCP()
		go d.HandleConnection(c)
	}
}


func (d *DHT) DoLockResponse(key, ts uint64, id []byte, sig byte) {
	if string(id) == string(d.ID) {

	}
}

func (d *DHT) DoLockRequest(c net.Conn, id string, key uint64, ts uint64) {
	if d.arr[key] == nil {
		d.arr[key] = new(entry)
	}
	e := d.arr[key]
	if e.lock == nil {
		e.lock = new(LockInfo)
		e.lock.Holder = id
		e.lock.time = ts
		//Now propogate out this message to each other node
	} else {
		if e.lock.Holder == id {
			//Write back YES to say that the lock it held
		} else {
			//Lock contention, resolve please
		}
	}
}

//attempts to get a lock on the given key string across the network, returns success
func (d *DHT) tryGetLock(onkey uint64) bool {
	//Send request for lock to each node that this node is connected to

	//Claim lock locally
	if d.arr[onkey] == nil {
		d.arr[onkey] = new(entry)
	}

	utime := uint64(time.Now().UnixNano())
	temp := d.arr[onkey]
	if temp.lock != nil {
		return false
	} else {
		temp.lock = new(LockInfo)
		temp.lock.Holder = string(d.ID)
		temp.lock.time = utime
	}

	lreq := MakeLockRequest(d.ID, onkey, utime)
	nreq := 0
	for _, c := range d.peerCons {
		c.Write(lreq)
		nreq++
	}
	d.lockReqs[onkey] = make(chan bool)
	//On each of those nodes, if the lock is available, take it, set the lockID to this nodes ID
	//send out lock requests to each node that they are connected to
	//If a node receives a lock request and the lock is already taken:
	//	check if the lock ID is this requesting node, if it is, return YES
	//	Otherwise, [resolve locking conflict]
	//Once all nodes return YES, return YES
	for ;nreq > 0; nreq-- {
		v := <-d.lockReqs[onkey]
		if !v {
			return false
			temp.lock = nil
		}
	}
	//If the nodes dont return YES in a certain time, return NO
	return true
}

func (d *DHT) releaseLock(onkey uint64) {
	lrel := MakeLockReleaseRequest(d.ID, onkey, uint64(time.Now().UnixNano()))

	if d.arr[onkey] == nil {
		d.arr[onkey] = new(entry)
	}

	temp := d.arr[onkey]
	if temp.lock == nil {
		return
	} else {
		if temp.lock.Holder == string(d.ID) {
			temp.lock = nil
		} else {
			panic("Cant release lock you dont own!")
		}
	}

	//Now propogate the release outwards
	d.sendToAll(lrel)
	//exit because we dont care about the release 'finishing'
}

func (d *DHT) sendToAll(b []byte) int {
	n := 0
	for _, c := range d.peerCons {
		c.Write(b)
		n++
	}
	return n
}

func (d *DHT) GetVal(key int) string {
	//Wait for any locks on this entry to resolve
	if d.arr[key] == nil {
		//SEND OUT REMINDER REQUEST
		d.remReqs[key] = make(chan string)
		d.sendToAll(MakeReminderRequest(key))
		val := <-d.remReqs[key]
		d.remReqs[key] = nil
		d.arr[key] = new(entry)
		d.arr[key].val = val
		return val
	}

	for d.arr[key].lock != nil {
		time.Sleep(time.Millisecond)
	}
	return d.arr[key].val
}

func (d *DHT) SetValue(key uint64, val string) {
	//First, request a lock on this entry across the network
	if d.tryGetLock(key) {
		//Set the value across the network
		d.sendToAll(MakeUpdateRequest(d.ID, key, val))
		//And then release the lock
		d.releaseLock(key)
	}
}

func (d *DHT) connectToPeer(addr string) {
	nAddr, _ := net.ResolveTCPAddr("tcp", addr)
	c, err := net.DialTCP("tcp", nil, nAddr)
	if err != nil {
		fmt.Printf("Failed to connect to %s.\n", addr)
		return
	}
	d.peerCons[d.numCons] = c
	d.numCons++
	//Do initial handshake
	//request hash table entries

}

func main() {
	d := NewDHT(1024)
	d.GetVal(0)
	//fmt.Println(m)
}
