package main

import (
	"net"
	"encoding/base64"
	"crypto/sha1"
	"os"
	"crypto/rand"
	"fmt"
	"encoding/gob"
	"time"
	"bytes"
	//"strconv"
)


type LockInfo struct {
	Holder string
	time int64
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

func NewDHT(size int, port string) *DHT {
	dht := new(DHT)
	dht.ID = GetUID()
	dht.arr = make([]*entry, size)
	dht.peerCons = make([]net.Conn,0,  128)
	dht.lockReqs = make(map[uint64]chan bool)
	go dht.Listen(port)
	return dht
}

func (d *DHT) HandleConnection(c net.Conn) {
	d.peerCons = d.peerCons[:len(d.peerCons)+1]
	d.peerCons[d.numCons] = c
	d.numCons++
	income := gob.NewDecoder(c)
	var mes Message
	for {
		d.Log("Waiting for message...")
		income.Decode(&mes)
		d.Log(fmt.Sprintf("Got message of type %d",mes.Type))
		switch mes.Type {
		case MLockReq:
			d.DoLockRequest(c, &mes)
		case MLockRelease:
			d.DoLockRelease(&mes)
		case MLockRespYes, MLockRespNo:
			d.DoLockResponse(&mes)
		case MUpdateVal:
			d.DoValueUpdate(&mes)
		case MRemindVal:
			d.DoRemind(&mes)
		case MRemindReq:
			d.DoReminder(c, &mes)
		default:
			d.Log("Received invalid message type")
		}
	}
}

func (d *DHT) DoLockRelease(mes *Message) {
	d.Log(fmt.Sprintf("Releasing key %d", mes.Key))
	if d.arr[mes.Key] == nil {
		panic("WHAT DO?")
	}

	e := d.arr[mes.Key]
	if e.lock != nil {

	}
}

//Accept 'reminder' which updates an entry only if that entry is null (never been set)
func (d *DHT) DoRemind(mes *Message) {
	if d.arr[mes.Key] == nil {
		m := new(entry)
		m.val = mes.Val
		d.arr[mes.Key] = m
	}
}

func (d *DHT) DoValueUpdate(mes *Message) {
	d.Log("In DoValueUpdate.")
	if d.arr[mes.Key] == nil {
		d.arr[mes.Key] = new(entry)
	}

	e := d.arr[mes.Key]
	if e.val != mes.Val {
		d.sendToAll(mes)
		e.val = mes.Val
	}
}

func (d *DHT) DoReminder(c net.Conn, mes *Message) {
	d.Log("In DoReminder")
	if d.arr[mes.Key] == nil {
		if d.remReqs[mes.Key] == nil {
			//Propogate request
		}
	} else {
		var nmes Message
		nmes.Key = mes.Key
		nmes.Val = d.arr[mes.Key].val
		nmes.Type = MRemindVal
		enc := gob.NewEncoder(c)
		enc.Encode(&nmes)
	}
}

func (d *DHT) Listen(port string) {
	addr, err := net.ResolveTCPAddr("tcp",port)
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

func (d *DHT) Log(message string) {
	fmt.Printf("%s: %s\n", d.PrintUID(), message)
}

func (d *DHT) DoLockResponse(mes *Message) {
	d.Log(fmt.Sprintf("Got lock response of %d for key %d", mes.Type, mes.Key))
	if string(mes.ID) == string(d.ID) {
		d.lockReqs[mes.Key] <- (mes.Type == MLockRespYes)
	} else {
		d.Log("ID's did not match")
		//Propogate outwards?
	}
}

func (d *DHT) DoLockRequest(c net.Conn, mes *Message) {
	d.Log(fmt.Sprintf("In DoLockRequest for key %d", mes.Key))
	if d.arr[mes.Key] == nil {
		d.arr[mes.Key] = new(entry)
	}
	e := d.arr[mes.Key]
	if e.lock == nil {
		e.lock = new(LockInfo)
		e.lock.Holder = string(mes.ID)
		e.lock.time = mes.Timestamp
		//Now propogate out this message to each other node
		if d.numCons == 1 {
			d.Log("Returning true to lock request.")
			enc := gob.NewEncoder(c)
			mes.Type = MLockRespYes
			enc.Encode(mes)
		} else {
			mes.Type = MLockReq
			d.sendToAll(mes)
		}
	} else {
		if e.lock.Holder == string(mes.ID) {
			//Write back YES to say that the lock it held
			d.Log("Returning true to lock request.")
			enc := gob.NewEncoder(c)
			mes.Type = MLockRespYes
			enc.Encode(mes)
		} else {
			//Lock contention, resolve please
		}
	}
}

//attempts to get a lock on the given key string across the network, returns success
func (d *DHT) tryGetLock(onkey uint64) bool {
	d.Log(fmt.Sprintf("Attempting to lock key %d", onkey))
	//Send request for lock to each node that this node is connected to

	//Claim lock locally
	if d.arr[onkey] == nil {
		d.arr[onkey] = new(entry)
	}

	utime := time.Now().UnixNano()
	temp := d.arr[onkey]
	if temp.lock != nil {
		return false
	} else {
		temp.lock = new(LockInfo)
		temp.lock.Holder = string(d.ID)
		temp.lock.time = utime
	}

	//lreq := MakeLockRequest(d.ID, onkey, utime)
	mes := new(Message)
	mes.Type = MLockReq
	mes.Key = onkey
	mes.ID = d.ID
	n := d.sendToAll(mes)
	d.lockReqs[onkey] = make(chan bool)
	//On each of those nodes, if the lock is available, take it, set the lockID to this nodes ID
	//send out lock requests to each node that they are connected to
	//If a node receives a lock request and the lock is already taken:
	//	check if the lock ID is this requesting node, if it is, return YES
	//	Otherwise, [resolve locking conflict]
	//Once all nodes return YES, return YES
	d.Log(fmt.Sprintf("Waiting on %d nodes", n))
	for ;n > 0; n-- {
		v := <-d.lockReqs[onkey]
		d.Log("Got lock return from channel")
		if !v {
			temp.lock = nil
			d.Log("Failed to get lock!")
			return false
		}
	}
	d.Log(fmt.Sprintf("Got lock for key %d", onkey))
	d.lockReqs[onkey] = nil
	//If the nodes dont return YES in a certain time, return NO
	return true
}

func (d *DHT) releaseLock(onkey uint64) {
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
	mes := new(Message)
	mes.Type = MLockRelease
	mes.ID = d.ID
	mes.Key = onkey
	mes.Timestamp = time.Now().UnixNano()
	d.sendToAll(mes)
	//exit because we dont care about the release 'finishing'
}

func (d *DHT) sendToAll(mes *Message) int {
	buf := new(bytes.Buffer)
	genc := gob.NewEncoder(buf)
	genc.Encode(mes)
	b := buf.Bytes()
	for i := 0; i < len(d.peerCons); i++ {
		n, err := d.peerCons[i].Write(b)
		if n == 0 {
			d.Log("Wrote zero bytes.")
		}
		if err != nil {
			d.Log(err.Error())
		}
	}
	return len(d.peerCons)
}

func (d *DHT) GetVal(key uint64) string {
	//Wait for any locks on this entry to resolve
	if d.arr[key] == nil {
		//SEND OUT REMINDER REQUEST
		d.remReqs[key] = make(chan string)
		var mes Message
		mes.ID = d.ID
		mes.Type = MRemindReq
		mes.Key = key
		d.sendToAll(&mes)
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
	d.Log(fmt.Sprintf("Setting value for key %d.", key))
	//First, request a lock on this entry across the network
	if d.tryGetLock(key) {
		d.Log("Got Lock!")
		//Set the value across the network
		var mes Message
		mes.ID = d.ID
		mes.Type = MUpdateVal
		mes.Key = key
		mes.Val = val
		d.sendToAll(&mes)
		//And then release the lock
		d.releaseLock(key)
	}
}

func (d *DHT) PrintUID() string {
	return base64.StdEncoding.EncodeToString(d.ID)
}

func (d *DHT) connectToPeer(addr string) {
	nAddr, _ := net.ResolveTCPAddr("tcp", addr)
	c, err := net.DialTCP("tcp", nil, nAddr)
	if err != nil {
		fmt.Printf("Failed to connect to %s.\n", addr)
		fmt.Println(err)
		return
	}
	//Do initial handshake
	//request hash table entries

	go d.HandleConnection(c)
}

func main() {
	sn := 32
	d := NewDHT(sn, os.Args[1])
	fmt.Printf("First ID: %s\n",d.PrintUID())
	da := NewDHT(sn, ":8080")
	fmt.Printf("Second ID: %s\n",da.PrintUID())
	db := NewDHT(sn, ":8181")

	time.Sleep(time.Second)
	d.connectToPeer("127.0.0.1:8080")
	db.connectToPeer("127.0.0.1:8080")
	/*
	for {
		i := Rand(int64(sn))
		time.Sleep(time.Second)
		d.SetValue(i, strconv.Itoa(int(i)))
	}
	*/
	time.Sleep(time.Second)
	d.SetValue(3, "THEFISH")
	time.Sleep(time.Second * 3)
	fmt.Println(da.arr[3].val)
	fmt.Println(da.arr[3].val)
	//fmt.Println(m)
}
