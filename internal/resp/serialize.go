package resp

import "strconv"

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

