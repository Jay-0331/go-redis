package redis

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/cache"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

type Role string

const (
	MASTER Role = "master"
	SLAVE Role = "slave"
)

type Node interface {
	Accept() net.Conn
	IsMaster() bool
	IsSlave() bool
	GetRole() Role
	GetCache() cache.Cache
	GetPort() string
	GetReplId() string
	SetReplId(string)
	GetRepOffset() int
	SetRepOffset(int)
	AddSlaveConn(net.Conn)
	GetSlaveConn() []Conn
	RemoveSlaveConn(net.Conn)
	GetMasterReplicaAddr() string
	GetMasterPort() string
	GetMasterConn() Conn
	GetOffset() int
	UpdateOffset(int)
	GetRDBFile() RDBfile
	GetRDBFileName() string
	GetRDBDir() string
	SetRDBFile(string, string)
}

type RDBfile struct {
	fileName string
	dir string
}

type NodeType struct {
	conn net.Listener
	host string
	port string
	isMaster bool
	isSlave bool
	role Role
	cache cache.Cache
	replId string
	repOffset int
	offSet int
	slaveConn []Conn
	masterConn Conn
	masterHost string
	masterPort string
	rdbFile RDBfile
}

func NewNode() Node {
	port := flag.String("port", "6379", "Port to bind to")
	masterHost := flag.String("replicaof", "", "Host of master Node")
	dir := flag.String("dir", "", "Directory to store RDB file")
	fileName := flag.String("dbfilename", "", "Name of RDB file")
	flag.Parse()
	rdbFile := RDBfile{
		fileName: *fileName,
		dir: *dir,
	}
	file, _ := os.Open(rdbFile.dir + "/" + rdbFile.fileName)
	buf := make([]byte, 1024)
	file.Read(buf)
	fmt.Println("RDB file content: ", string(buf))
	file.Close()
	masterPort := flag.Arg(0)
	addr := "0.0.0.0:" + *port
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port: ", err.Error())
		return nil
	}
	node := Node(nil)
	if *masterHost != "" {
		node = NewSlave(l, "0.0.0.0", *port, *masterHost, masterPort, rdbFile)	
	} else {
		node = newMaster(l, "0.0.0.0", *port, rdbFile)
	}
	if rdbFile.fileName != "" || rdbFile.dir != "" {
		data := resp.LoadValuesFromRDBFile(rdbFile.dir + "/" + rdbFile.fileName)
		cache := node.GetCache()
		fmt.Println(data)
		for _, value := range data {
			cache.Set(value.Key, value.Value, value.ExpireTime)
		}
	}
	return node
}

func newMaster(c net.Listener, host, port string, rdbFile RDBfile) Node {
	return &NodeType{
		conn: c,
		host: host,
		port: port,
		isMaster: true,
		isSlave: false,
		role: MASTER,
		cache: cache.NewCache(),
		replId:    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		repOffset: 0,
		rdbFile: rdbFile,
	}
}

func NewSlave(c net.Listener, host, port, masterHost, masterPort string, rdbFile RDBfile) Node {
	s := &NodeType{
		conn: c,
		host: host,
		port: port,
		isMaster: false,
		isSlave: true,
		role: SLAVE,
		cache: cache.NewCache(),
		replId:    "",
		repOffset: 0,
		masterHost: masterHost,
		masterPort: masterPort,
		masterConn: nil,
		rdbFile: rdbFile,
	}
	masterConn, err := net.Dial("tcp", s.GetMasterReplicaAddr())
	if err != nil {
		panic("Failed to connect to master: " + err.Error())
	}
	s.masterConn = newConn(masterConn)
	s.handShake()
	return s
}

func (n *NodeType) Accept() net.Conn {
	conn, err := n.conn.Accept()
	if err != nil {
		fmt.Println("Failed to accept connection: ", err.Error())
		os.Exit(1)
	}
	return conn
}

func (n *NodeType) IsMaster() bool {
	return n.isMaster
}

func (n *NodeType) IsSlave() bool {
	return n.isSlave
}

func (n *NodeType) GetRole() Role {
	return n.role
}

func (n *NodeType) GetCache() cache.Cache {
	return n.cache
}

func (n *NodeType) GetPort() string {
	return n.port
}

func (n *NodeType) GetReplId() string {
	return n.replId
}

func (n *NodeType) SetReplId(replId string) {
	n.replId = replId
}

func (n *NodeType) GetRepOffset() int {
	return n.repOffset
}

func (n *NodeType) SetRepOffset(repOffset int) {
	n.repOffset = repOffset
}

func (n *NodeType) GetOffset() int {
	return n.offSet
}

func (n *NodeType) UpdateOffset(offset int) {
	n.offSet += offset
}

func (n *NodeType) AddSlaveConn(conn net.Conn){
	n.slaveConn = append(n.slaveConn, newConn(conn))
}

func (n *NodeType) GetSlaveConn() []Conn {
	return n.slaveConn
}

func (n *NodeType) RemoveSlaveConn(conn net.Conn) {
	for i, sc := range n.slaveConn {
		if sc.GetConn() == conn {
			n.slaveConn = append(n.slaveConn[:i], n.slaveConn[i+1:]...)
			break
		}
	}
}

func (n *NodeType) GetMasterReplicaAddr() string {
	return n.masterHost + ":" + n.masterPort
}

func (n *NodeType) GetMasterPort() string {
	return n.masterPort
}

func (n *NodeType) GetMasterConn() Conn {
	return n.masterConn
}

func (n *NodeType) handShake() {
	// PING REQUEST
	n.reqToMaster("*1\r\n$4\r\nping\r\n", "+PONG\r\n")
	// Replication Configuration REQUEST
	n.reqToMaster("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n", "+OK\r\n")
	n.reqToMaster("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", "+OK\r\n")
	// PSYNC REQUEST
	n.reqToMaster("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", "")
}

func (n *NodeType) reqToMaster(req, want string) {
	n.masterConn.Write([]byte(req))
	if want != "" {
		buf := make([]byte, 1024)
		numBytes, err := n.masterConn.Read(buf)
		if err != nil {
			panic(err)
		}
		fmt.Println("Received from master redis.go:", string(buf[:numBytes]))

		if string(buf[:numBytes]) != want {
			panic("Unexpected response from master")
		}
	}
}

func (n *NodeType) GetRDBFile() RDBfile {
	return n.rdbFile
}

func (n *NodeType) GetRDBFileName() string {
	return n.rdbFile.fileName
}

func (n *NodeType) GetRDBDir() string {
	return n.rdbFile.dir
}

func (n *NodeType) SetRDBFile(fileName, dir string) {
	n.rdbFile.fileName = fileName
	n.rdbFile.dir = dir
}