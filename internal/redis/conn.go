package redis

import (
	"fmt"
	"net"
	"sync"
)

type Conn interface {
	Write(p []byte)
	Read(p []byte) (int, error)
	GetConn() net.Conn
}

type connType struct {
	mu sync.Mutex
	conn net.Conn
}

func newConn(conn net.Conn) Conn {
	return &connType{
		conn: conn,
	}
}

func (c *connType) Write(p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.conn.Write(p)
	if err != nil {
		fmt.Println("Error writing to slave:", err.Error())
	}
	if n != len(p) {
		fmt.Println("Failed to write all bytes to slave")
	}
}

func (c *connType) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.conn.Read(p)
	return n, err
}

func (c *connType) GetConn() net.Conn {
	return c.conn
}