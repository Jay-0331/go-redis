package util

import (
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal/command"
	"github.com/codecrafters-io/redis-starter-go/internal/redis"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

func Execute(redis redis.Node, conn net.Conn, cmd command.Command) {
	c := redis.GetCache()
	switch cmd.GetName() {
	case command.PING:
		if redis.IsSlave() {
			redis.UpdateOffset(len(resp.ToRESPArray(cmd.CmdToSlice())))
		} else {
			conn.Write([]byte(handlePing()))
		}
	case command.ECHO:
		conn.Write([]byte(handleEcho(cmd)))
	case command.TYPE:
		conn.Write([]byte(handleType(cmd, c)))
	case command.XADD:
		res := handleXADD(cmd, c, redis)
		if redis.IsSlave() {
			redis.UpdateOffset(len(resp.ToRESPArray(cmd.CmdToSlice())))
		} else {
			conn.Write([]byte(res))
		}
	case command.XRANGE:
		conn.Write([]byte(handleXRANGE(cmd, c)))
	case command.XREAD:
		conn.Write([]byte(handleXREAD(cmd, c)))
	case command.KEYS:
		conn.Write([]byte(handleKeys(c)))
	case command.SET:
		res := handleSet(cmd, c)
		if redis.IsSlave() {
			redis.UpdateOffset(len(resp.ToRESPArray(cmd.CmdToSlice())))
		} else {
			conn.Write([]byte(res))
		}
	case command.GET:
		conn.Write([]byte(handleGet(cmd, c)))
	case command.INFO:
		conn.Write([]byte(handleInfo(cmd, c, redis)))
	case command.REPLCONF:
		res := handleReplConf(cmd, redis, conn)
		if redis.IsMaster() && res != "" {
			conn.Write([]byte(res))
		}
	case command.PSYNC:
		if redis.IsMaster() {
			conn.Write([]byte(handlePSYNC(cmd, redis, conn)))
		} else {
			conn.Write([]byte(resp.ToRESPError("Invalid Configuration")))
		}
	case command.WAIT:
		conn.Write([]byte(handleWait(cmd, redis)))
	case command.CONFIG:
		conn.Write([]byte(handleConfig(cmd, redis)))
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