package resp

import (
	"bufio"
	"strconv"
	"strings"
)

type RespType [][]string

type RespScanner struct {
	*bufio.Scanner
}

func NewResp(data []byte) RespType {
	respScanner := newRespScanner(data)
	resp := respScanner.Parse()
	return resp
}

func newRespScanner(data []byte) *RespScanner {
	return &RespScanner{
		Scanner: bufio.NewScanner(strings.NewReader(string(data))),
	}
}

func (scanner *RespScanner) Parse() RespType {
	resp := make(RespType, 0)
	for scanner.Scan() {
		line := scanner.Text()
		length, _ := strconv.Atoi(line[1:])
		switch line[0] {
		case '*':
			resp = append(resp, scanner.parseArray(length))
		case '$':
			scanner.Scan()
			scanner.Scan()
			temp := strings.Split(scanner.Text(), "*")
			if len(temp) == 2 {
				arrlen, _ := strconv.Atoi(temp[1])
				resp = append(resp, scanner.parseArray(arrlen))
			}
		}
	}
	return resp
}

func (scanner *RespScanner) parseArray(length int) []string {
	resp := make([]string, 0)
	for i := 0; i < length; i++ {
		resp = append(resp, scanner.parseBulkString())
	}
	return resp
}

func (scanner *RespScanner) parseBulkString() string {
	scanner.Scan()
	scanner.Scan()
	str := scanner.Text()
	return strings.ToLower(str)
}