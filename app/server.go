package main

import (
	"fmt"
	"io"
	"net"

	"github.com/codecrafters-io/redis-starter-go/internal/command"
	"github.com/codecrafters-io/redis-starter-go/internal/redis"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/util"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.

	// Uncomment this block to pass the first stage
	// port := flag.String("port", "6379", "Port to bind to")
	redis := redis.NewNode()
	for {
		if redis.IsSlave() {
			go handleMasterConnection(redis)
		}
		conn := redis.Accept()
		go handleConnection(redis, conn)
	}
}

func handleConnection(redis redis.Node, conn net.Conn) {
	// Implement the Redis protocol here
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err == io.EOF {
			fmt.Println("Connection closed")
			return
		}
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			return
		}
		inputArr := resp.NewResp(buf[:n])
		for _, input := range inputArr {
			cmd, err := command.NewCommand(input)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			util.Execute(redis, conn, *cmd)
		}
	}
}

func handleMasterConnection(redis redis.Node) {
	defer redis.GetMasterConn().GetConn().Close()
	defer redis.RemoveSlaveConn(redis.GetMasterConn().GetConn())
	repBuf := make([]byte, 1024)
	for {
		n, err := redis.GetMasterConn().Read(repBuf)
		if err == io.EOF {
			fmt.Println("Connection closed")
			return
		}
		if err != nil {
			fmt.Println("Error reading from master:", err.Error())
			return
		}
		inputArr := resp.NewResp(repBuf[:n])
		for _, input := range inputArr {
			cmd, err := command.NewCommand(input)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			util.ExecuteReplica(redis, *cmd)
		}
	}
}