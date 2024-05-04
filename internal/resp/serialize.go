package resp

import (
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal/cache"
)

const (
	CLRF = "\r\n"
)

func ToRESPArray(arr []string) string {
	resp := "*" + strconv.Itoa(len(arr)) + CLRF
	for _, str := range arr {
		resp += ToRESPBulkString(str)
	}
	return resp
}

func ToRESPSimpleString(str string) string {
	return "+" + str + CLRF
}

func ToRESPError(err string) string {
	return "-" + err + CLRF
}

func ToRESPInteger(i int) string {
	return ":" + strconv.Itoa(i) + CLRF
}	

func ToRESPBulkString(str string) string {
	return "$" + strconv.Itoa(len(str)) + CLRF + str + CLRF
}

func ToRESPNullBulkString() string {
	return "$-1" + CLRF
}

func ToRESPNullArray() string {
	return "*-1" + CLRF
}

func ToStreamRESPArray(arr []cache.StreamType) string {
	resp := "*" + strconv.Itoa(len(arr)) + CLRF
	for _, stream := range arr {
		resp += "*" + "2" + CLRF
		resp += ToRESPBulkString(stream.Id)
		resp += ToRESPArray(stream.Data)
	}
	return resp
}