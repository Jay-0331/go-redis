package util

import (
	"encoding/base64"
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

func handleSet(cmd command.Command, c cache.Cache) string {
	if len(cmd.GetArgs()) > 2 && cmd.GetArg(2) == "px" {
		px, err := strconv.Atoi(cmd.GetArg(3))
		if err != nil {
			return resp.ToRESPError("Invalid Argument")
		}
		c.Set(cmd.GetArg(0), cmd.GetArg(1), int64(px))
	} else if len(cmd.GetArgs()) == 3 {
		return resp.ToRESPError("Invalid Argument")
	} else {
		c.Set(cmd.GetArg(0), cmd.GetArg(1), 0)
	}
	return resp.ToRESPSimpleString("OK")
}

func handleGet(cmd command.Command, c cache.Cache) string {
	value, err := c.Get(cmd.GetArg(0))
	if err != nil {
		return resp.ToRESPError("Key does not exist")
	}
	return resp.ToRESPBulkString(value)
}

func handleKeys(c cache.Cache) string {
	keys := c.Keys()
	return resp.ToRESPArray(keys)
}

func handleDel(cmd command.Command, c cache.Cache) string {
	c.Del(cmd.GetArg(0))
	return resp.ToRESPSimpleString("OK")
}

func handleInfo(cmd command.Command, c cache.Cache, redis redis.Node) string {
	role := "role:" + string(redis.GetRole())
			if redis.IsMaster() {
				master_id := "master_replid:" + redis.GetReplId()
				master_offset := "master_repl_offset:" + strconv.Itoa(redis.GetRepOffset())
				return resp.ToRESPBulkString(role + "\n" + master_id + "\n" + master_offset)
			} 
			return resp.ToRESPBulkString(role)
}

func handleReplConf(cmd command.Command, redis redis.Node, conn net.Conn) string {
	if cmd.GetArg(0) == "listening-port" {
		redis.AddSlaveConn(conn)
		return resp.ToRESPSimpleString("OK")
	} else if cmd.GetArg(0) == "capa" {
		return resp.ToRESPSimpleString("OK")
	} else if cmd.GetArg(0) == "getack" {
		propagate(redis, cmd)
		if redis.IsSlave() {
			if cmd.GetArg(0) == "getack" {
				resp := resp.ToRESPArray([]string{"REPLCONF", "ACK", strconv.Itoa(redis.GetOffset())})
				redis.GetMasterConn().Write([]byte(resp))
			}
			redis.UpdateOffset(len(resp.ToRESPArray(cmd.CmdToSlice())))
			return ""
		}
	} else if cmd.GetArg(0) == "ack" {
		ackChan <- true
		return ""
	} else {
		return resp.ToRESPError("Invalid Configuration")
	}
}

func handlePSYNC(cmd command.Command, redis redis.Node, conn net.Conn) string {
	if cmd.GetArg(0) == "?" && cmd.GetArg(1) == "-1"{
		conn.Write([]byte(resp.ToRESPSimpleString("+FULLRESYNC " + redis.GetReplId() + " " + strconv.Itoa(redis.GetRepOffset()))))
		emptyRDB, err := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
		if err != nil {
			return resp.ToRESPError(err.Error())
		}
		return string(emptyRDB)
	}
	return resp.ToRESPError("Invalid Command")
}

func handleWait(cmd command.Command, redis redis.Node) string {
	numConn := len(redis.GetSlaveConn())
	needAck, _ := strconv.Atoi(cmd.GetArg(0))
	timeout, _ := strconv.Atoi(cmd.GetArg(1))
	
	if needAck != 0 {
		ackCmd, err := command.NewCommand([]string{"REPLCONF", "GETACK", "*"})
		if err != nil {
			return resp.ToRESPError(err.Error())
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
					return resp.ToRESPInteger(numAck)
				}
			case <- time.After(time.Duration(timeout) * time.Millisecond):
				if numAck > 0 {
					return resp.ToRESPInteger(numAck)
				}
				return resp.ToRESPInteger(numConn)
			}
		}
	}
	return resp.ToRESPInteger(numConn)
}

func handlePing() string {
	return resp.ToRESPSimpleString("PONG")
}

func handleEcho(cmd command.Command) string {
	return resp.ToRESPBulkString(cmd.GetArg(0))
}

func handleType(cmd command.Command, c cache.Cache) string {
	return resp.ToRESPSimpleString(c.GetType(cmd.GetArg(0)))
}

func handleConfig(cmd command.Command, redis redis.Node) string {
	if cmd.GetArg(0) == "get" {
		if cmd.GetArg(1) == "dir" {
			return resp.ToRESPArray([]string{cmd.GetArg(1), redis.GetRDBDir()})
		}
	}
	return resp.ToRESPError("Invalid Command")
}

func handleXADD(cmd command.Command, c cache.Cache, redis redis.Node) string {
	go propagate(redis, cmd)
	c.SetStream(cmd.GetArg(0))
	id, err := c.AddToStream(cmd.GetArg(0), cmd.GetArg(1), cmd.GetArgs()[2:])
	if err != nil {
		return resp.ToRESPError(err.Error())
	}
	pubSub.Publish("xread", cmd.GetArg(0) + "_" + id)
	return resp.ToRESPBulkString(id)
}

func handleXRANGE(cmd command.Command, c cache.Cache) string {
	stream := c.GetStream(cmd.GetArg(0), cmd.GetArg(1), cmd.GetArg(2))
	if len(stream) == 0 {
		return resp.ToRESPNullArray()
	}
	return resp.ToStreamRESPArray(stream)
}

func handleXREAD(cmd command.Command, c cache.Cache) string {
	switch cmd.GetArg(0) {
	case "streams":
		streamMap := GetStreamMap(cmd.GetArgs()[1:], c)
		if len(streamMap) == 0 {
			return resp.ToRESPNullArray()
		}
		return resp.ToRESPStreamWithName(streamMap)
	case "block":
		timeout, _ := strconv.Atoi(cmd.GetArg(1))
		streamMap := make(map[string][]cache.StreamType)
		agrsSet := map[string]bool{}
		needs := len(cmd.GetArgs()[3:]) / 2
		for _, arg := range cmd.GetArgs()[3: needs + 3] {
			agrsSet[arg] = true
		}
		resposeChan := make(chan string)
		go handleBlockXREAD(timeout, c, streamMap, agrsSet, resposeChan)
		return <- resposeChan
	default:
		return resp.ToRESPError("Invalid Command")
	}
}

func handleBlockXREAD(timeout int, cache cache.Cache, streamMap map[string][]cache.StreamType, args map[string]bool, resposeChan chan string) {
	xread_chan := pubSub.Subscribe("xread")
	defer pubSub.Unsubscribe("xread")
	for	{
		if timeout == 0 {
			message := <- xread_chan
			AddToStreamMap(message, cache, streamMap, args)
			if len(streamMap) == len(args) {
				resposeChan <- resp.ToRESPStreamWithName(streamMap)
			}
		} else {
			select {
			case <- time.After(time.Duration(timeout) * time.Millisecond):
				resposeChan <- resp.ToRESPNullArray()
			case message := <- xread_chan:
				AddToStreamMap(message, cache, streamMap, args)
				if len(streamMap) == len(args) {
					resposeChan <- resp.ToRESPStreamWithName(streamMap)
				}
			}
		}
	}
}

func AddToStreamMap(message PubSubMessage, cache cache.Cache, streamMap map[string][]cache.StreamType, args map[string]bool) {
	parts := strings.Split(message.Message, "_")
	key := parts[0]
	id := parts[1]
	stream := cache.GetStream(key, id, "+")
	if len(stream) == 0 {
		return
	}
	if _, ok := args[key]; ok {
		streamMap[key] = stream
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