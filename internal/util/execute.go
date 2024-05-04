package util

import (
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/command"
	"github.com/codecrafters-io/redis-starter-go/internal/redis"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

var ackChan = make(chan bool, 1)

func Execute(redis redis.Node, conn net.Conn, cmd command.Command) {
	cache := redis.GetCache()
	switch cmd.GetName() {
	case command.PING:
		conn.Write([]byte(resp.ToRESPSimpleString("PONG")))
		return
	case command.ECHO:
		conn.Write([]byte(resp.ToRESPBulkString(cmd.GetArg(0))))
		return
	case command.TYPE:
		valueType := cache.GetType(cmd.GetArg(0))
		switch valueType {
		case "string":
			conn.Write([]byte(resp.ToRESPSimpleString("string")))
		case "stream":
			conn.Write([]byte(resp.ToRESPSimpleString("stream")))
		default:
			conn.Write([]byte(resp.ToRESPSimpleString("none")))
		}
	case command.XADD:
		go propagate(redis, cmd)
		cache.SetStream(cmd.GetArg(0))
		id, err := cache.AddToStream(cmd.GetArg(0), cmd.GetArg(1), cmd.GetArgs()[2:])
		if err != nil {
			conn.Write([]byte(resp.ToRESPError(err.Error())))
			return
		}
		conn.Write([]byte(resp.ToRESPSimpleString(id)))
	case command.KEYS:
		keys := cache.Keys()
		fmt.Println(keys)
		conn.Write([]byte(resp.ToRESPArray(cache.Keys())))
	case command.SET:
		go propagate(redis, cmd)
		if len(cmd.GetArgs()) > 2 && cmd.GetArg(2) == "px" {
			px, err := strconv.Atoi(cmd.GetArg(3))
			if err != nil {
				conn.Write([]byte(resp.ToRESPError(err.Error())))
			}
			cache.Set(cmd.GetArg(0), cmd.GetArg(1), int64(px))
			conn.Write([]byte(resp.ToRESPSimpleString("OK")))
		} else if len(cmd.GetArgs()) == 3 {
			conn.Write([]byte(resp.ToRESPError("Invalid Command")))
			return
		} else {
			cache.Set(cmd.GetArg(0), cmd.GetArg(1), 0)
			conn.Write([]byte(resp.ToRESPSimpleString("OK")))
		}
		case command.GET:
			value := cache.Get(cmd.GetArg(0))
			if value != "" {
				conn.Write([]byte(resp.ToRESPBulkString(value)))
			} else {
				conn.Write([]byte(resp.ToRESPNullBulkString()))
			}
			return
		case command.INFO:
			role := "role:" + string(redis.GetRole())
			if redis.IsMaster() {
				master_id := "master_replid:" + redis.GetReplId()
				master_offset := "master_repl_offset:" + strconv.Itoa(redis.GetRepOffset())
				conn.Write([]byte(resp.ToRESPBulkString(role + "\n" + master_id + "\n" + master_offset)))
			} else {
				conn.Write([]byte(resp.ToRESPBulkString(role)))
			}
			return
		case command.REPLCONF:
			if redis.IsMaster() {
				if cmd.GetArg(0) == "listening-port" {
					redis.AddSlaveConn(conn)
					conn.Write([]byte(resp.ToRESPSimpleString("OK")))
				} else if cmd.GetArg(0) == "capa" {
					conn.Write([]byte(resp.ToRESPSimpleString("OK")))
				} else if cmd.GetArg(0) == "getack" {
					propagate(redis, cmd)
				} else if cmd.GetArg(0) == "ack" {
					ackChan <- true
				} else {
					conn.Write([]byte(resp.ToRESPError("Invalid Configuration")))
				}
			}
		case command.PSYNC:
			if redis.IsMaster() {
				if cmd.GetArg(0) == "?" && cmd.GetArg(1) == "-1"{
					conn.Write([]byte(resp.ToRESPSimpleString("+FULLRESYNC " + redis.GetReplId() + " " + strconv.Itoa(redis.GetRepOffset()))))
					emptyRDB, err := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
					if err != nil {
						conn.Write([]byte(resp.ToRESPError(err.Error())))
						return
					}
					for _, slave := range redis.GetSlaveConn() {
						if slave.GetConn() == conn {
							conn.Write([]byte("$" + strconv.Itoa(len(string(emptyRDB))) + "\r\n" + string(emptyRDB)))
						}
					}
				}
			} else {
				conn.Write([]byte(resp.ToRESPError("Invalid Configuration")))
			}
		case command.WAIT:
			numConn := len(redis.GetSlaveConn())
			needAck, _ := strconv.Atoi(cmd.GetArg(0))
			timeout, _ := strconv.Atoi(cmd.GetArg(1))
			
			if needAck != 0 {
				ackCmd, err := command.NewCommand([]string{"REPLCONF", "GETACK", "*"})
				if err != nil {
					conn.Write([]byte(resp.ToRESPError(err.Error())))
					return
				}
				go propagate(redis, *ackCmd)
			}

			numAck := 0
			if needAck != 0 {
			for numAck < needAck {
				select {
				case <- ackChan:
					numAck++
					if numAck >= needAck {
						conn.Write([]byte(resp.ToRESPInteger(numAck)))
					}
				case <- time.After(time.Duration(timeout) * time.Millisecond):
					if numAck > 0 {
						conn.Write([]byte(resp.ToRESPInteger(numAck)))
						break
					}
					conn.Write([]byte(resp.ToRESPInteger(numConn)))
				}
			}
			} else {
				conn.Write([]byte(resp.ToRESPInteger(numConn)))
			}
		case command.CONFIG:
			switch cmd.GetArg(0) {
			case "get":
				if cmd.GetArg(1) == "dir" {
					conn.Write([]byte(resp.ToRESPArray([]string{cmd.GetArg(1), redis.GetRDBDir()})))
				}
			}
		default:
			conn.Write([]byte(resp.ToRESPError("Invalid Command")))
		}
}

func ExecuteReplica(redis redis.Node, cmd command.Command) {
	cache := redis.GetCache()
	switch cmd.GetName() {
	case command.PING:
		redis.UpdateOffset(len(resp.ToRESPArray(cmd.CmdToSlice())))
		return
	case command.SET:
		propagate(redis, cmd)
		if len(cmd.GetArgs()) > 2 && cmd.GetArg(2) == "px" {
			px, err := strconv.Atoi(cmd.GetArg(3))
			if err != nil {
				return
			}
			cache.Set(cmd.GetArg(0), cmd.GetArg(1), int64(px))
		} else if len(cmd.GetArgs()) == 3 {
			return
		} else {
			cache.Set(cmd.GetArg(0), cmd.GetArg(1), 0)
		}
		redis.UpdateOffset(len(resp.ToRESPArray(cmd.CmdToSlice())))
		return
	case command.REPLCONF:
		if cmd.GetArg(0) == "getack" {
			resp := resp.ToRESPArray([]string{"REPLCONF", "ACK", strconv.Itoa(redis.GetOffset())})
			redis.GetMasterConn().Write([]byte(resp))
		}
		redis.UpdateOffset(len(resp.ToRESPArray(cmd.CmdToSlice())))
		return
	}
}

func propagate(redis redis.Node, cmd command.Command) {
	for _, slave := range redis.GetSlaveConn() {
		slave.Write([]byte(resp.ToRESPArray(cmd.CmdToSlice())))
	}
}