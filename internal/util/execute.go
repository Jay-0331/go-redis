package util

import (
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/cache"
	"github.com/codecrafters-io/redis-starter-go/internal/command"
	"github.com/codecrafters-io/redis-starter-go/internal/redis"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

var ackChan = make(chan bool, 1)
var pubSub = NewPubSub()

func Execute(redis redis.Node, conn net.Conn, cmd command.Command) {
	c := redis.GetCache()
	switch cmd.GetName() {
	case command.PING:
		conn.Write([]byte(resp.ToRESPSimpleString("PONG")))
		return
	case command.ECHO:
		conn.Write([]byte(resp.ToRESPBulkString(cmd.GetArg(0))))
		return
	case command.TYPE:
		valueType := c.GetType(cmd.GetArg(0))
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
		c.SetStream(cmd.GetArg(0))
		id, err := c.AddToStream(cmd.GetArg(0), cmd.GetArg(1), cmd.GetArgs()[2:])
		if err != nil {
			conn.Write([]byte(resp.ToRESPError(err.Error())))
			return
		}
		conn.Write([]byte(resp.ToRESPSimpleString(id)))
		pubSub.Publish("xread", cmd.GetArg(0) + "_" + id)
	case command.XRANGE:
		stream := c.GetStream(cmd.GetArg(0), cmd.GetArg(1), cmd.GetArg(2))
		if len(stream) == 0 {
			conn.Write([]byte(resp.ToRESPNullArray()))
			return
		}
		conn.Write([]byte(resp.ToStreamRESPArray(stream)))
	case command.XREAD:
		switch cmd.GetArg(0) {
		case "streams":
			streamMap := GetStreamMap(cmd.GetArgs()[1:], c)
			if len(streamMap) == 0 {
				conn.Write([]byte(resp.ToRESPNullArray()))
				return
			}
			resp := resp.ToRESPStreamWithName(streamMap)
			conn.Write(([]byte(resp)))
		case "block":
			timeout, _ := strconv.Atoi(cmd.GetArg(1))
			streamMap := make(map[string][]cache.StreamType)
			needs := len(cmd.GetArgs()[3:]) / 2
			go func(timeout int, cache cache.Cache, streamMap map[string][]cache.StreamType, needs int) {
				xread_chan := pubSub.Subscribe("xread")
				defer pubSub.Unsubscribe("xread")
				for	{
					if timeout == 0 {
						message := <- xread_chan
						parts := strings.Split(message.Message, "_")
						key := parts[0]
						id := parts[1]
						stream := cache.GetStream(key, id, "+")
						if len(stream) == 0 {
							continue
						}
						streamMap[key] = stream
						if len(streamMap) == needs {
							resp := resp.ToRESPStreamWithName(streamMap)
							conn.Write(([]byte(resp)))
							break
						}
					} else {
						select {
						case <- time.After(time.Duration(timeout) * time.Millisecond):
							conn.Write([]byte(resp.ToRESPNullArray()))
						case message := <- xread_chan:
							parts := strings.Split(message.Message, "_")
							key := parts[0]
							id := parts[1]
							stream := cache.GetStream(key, id, "+")
							if len(stream) == 0 {
								continue
							}
							streamMap[key] = stream
						}
						if len(streamMap) == needs {
							resp := resp.ToRESPStreamWithName(streamMap)
							conn.Write(([]byte(resp)))
							break
						} else if timeout == 0 {
							continue
						}
					}
				}
				conn.Write([]byte(resp.ToRESPNullArray()))
			}(timeout, c, streamMap, needs)
			// go func(timeout int) {
			// 	prevStreamMap := GetStreamMap(cmd.GetArgs()[3:], c)
			// 	time.Sleep(time.Duration(timeout) * time.Millisecond)
			// 	loop: for {
			// 		streamMap := GetStreamMap(cmd.GetArgs()[3:], c)
			// 		if len(streamMap) == 0 {
			// 			conn.Write([]byte(resp.ToRESPNullArray()))
			// 			return
			// 		}
			// 		for key, stream := range streamMap {
			// 			streamMap[key] = stream[len(prevStreamMap[key]):]
			// 		}
			// 		completed := 0
			// 		for _, stream := range streamMap {
			// 			if timeout == 0 && len(stream) == 0 {
			// 				completed++
			// 				continue
			// 			} 
			// 			if len(stream) == 0 {
			// 				conn.Write([]byte(resp.ToRESPNullArray()))
			// 				break loop
			// 			} 
			// 		}
			// 		if completed == len(streamMap) {
			// 			continue loop
			// 		}
			// 		resp := resp.ToRESPStreamWithName(streamMap)
			// 		conn.Write(([]byte(resp)))
			// 		break loop
			// 	}
			// }(timeout)
		}
	case command.KEYS:
		keys := c.Keys()
		fmt.Println(keys)
		conn.Write([]byte(resp.ToRESPArray(c.Keys())))
	case command.SET:
		go propagate(redis, cmd)
		if len(cmd.GetArgs()) > 2 && cmd.GetArg(2) == "px" {
			px, err := strconv.Atoi(cmd.GetArg(3))
			if err != nil {
				conn.Write([]byte(resp.ToRESPError(err.Error())))
			}
			c.Set(cmd.GetArg(0), cmd.GetArg(1), int64(px))
			conn.Write([]byte(resp.ToRESPSimpleString("OK")))
		} else if len(cmd.GetArgs()) == 3 {
			conn.Write([]byte(resp.ToRESPError("Invalid Command")))
			return
		} else {
			c.Set(cmd.GetArg(0), cmd.GetArg(1), 0)
			conn.Write([]byte(resp.ToRESPSimpleString("OK")))
		}
		case command.GET:
			value := c.Get(cmd.GetArg(0))
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
	c := redis.GetCache()
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
			c.Set(cmd.GetArg(0), cmd.GetArg(1), int64(px))
		} else if len(cmd.GetArgs()) == 3 {
			return
		} else {
			c.Set(cmd.GetArg(0), cmd.GetArg(1), 0)
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
	case command.XADD:
		go propagate(redis, cmd)
		c.SetStream(cmd.GetArg(0))
		_, err := c.AddToStream(cmd.GetArg(0), cmd.GetArg(1), cmd.GetArgs()[2:])
		if err != nil {
			return
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

func GetStreamMap(streamArgs []string, c cache.Cache) map[string][]cache.StreamType {
	m := len(streamArgs) / 2
	streamMap := make(map[string][]cache.StreamType)
	for i := 0; i < m; i++ {
		key := streamArgs[i]
		start := streamArgs[i + m]
		stream := c.GetStream(key, start, "+")
		streamMap[key] = stream
	}
	return streamMap
}