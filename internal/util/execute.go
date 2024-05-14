package util

import (
	"net"

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
		res := handleSet(cmd, c, redis)
		if redis.IsSlave() {
			redis.UpdateOffset(len(resp.ToRESPArray(cmd.CmdToSlice())))
		} else {
			conn.Write([]byte(res))
		}
	case command.DEL:
		conn.Write([]byte(handleDel(cmd, c)))
	case command.GET:
		conn.Write([]byte(handleGet(cmd, c)))
	case command.INFO:
		conn.Write([]byte(handleInfo(redis)))
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