package resp

import (
	"encoding/binary"
	"os"
	"strconv"
	"time"
)

type RDBData struct {
	Key string
	Value string
	ExpireTime int64
}

type RDB struct {
	file *os.File
	selectedDB int
	hashTableSize int
	expireHashTableSize int
	data []RDBData
}

const (
	OP_EOF = 0xFF
	OP_SELECTDB = 0xFE
	OP_EXPIRETIME_MS = 0xFC
	OP_EXPIRETIME = 0xFD
	OP_RESIZEDB = 0xFB
	OP_AUX = 0xFA
)

func LoadValuesFromRDBFile(filePath string) []RDBData {
	rdb := RDB{
		data: []RDBData{},
	}
	rdb.OpenRDBFile(filePath)
	if !rdb.VerifyRDBFile() {
		return nil
	}
	main:
	for {
		opCode := rdb.ReadOpCode()
		switch opCode {
		case OP_EOF:
			break main
		case OP_SELECTDB:
			rdb.DatabaseSelect()
		case OP_RESIZEDB:
			rdb.ResizeDB()
		case OP_AUX:
			rdb.Aux()
		}
	}
	rdb.CloseRDBFile()
	return rdb.data
}

func (rdb *RDB) OpenRDBFile(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	rdb.file = file
}

func (rdb *RDB) VerifyRDBFile() bool {
	sanityCheckByte := make([]byte, 9)
	_, err := rdb.file.Read(sanityCheckByte)
	if err != nil {
		return false
	}
	if string(sanityCheckByte[:5]) != "REDIS" {
		return false
	}
	version, err := strconv.Atoi(string(sanityCheckByte[5:]))
	if err != nil {
		return false
	}
	if version < 2 || version > 7 {
		return false
	}
	return true
}

func (rdb *RDB) DatabaseSelect() {
	selectedDB := rdb.ReadLengthEncoded()
	rdb.selectedDB = selectedDB
}

func (rdb *RDB) ReadOpCode() byte {
	opCodeByte := make([]byte, 1)
	rdb.file.Read(opCodeByte)
	return opCodeByte[0]
}

func (rdb *RDB) ReadByte() (byte, error) {
	byte := make([]byte, 1)
	_, err := rdb.file.Read(byte)
	if err != nil {
		return 0, err
	}
	return byte[0], nil
}

func (rdb *RDB) ReadKeyVal() {
	expireTime := int64(0)
	switch rdb.ReadOpCode() {
	case OP_EXPIRETIME_MS:
		expireTimeByte := make([]byte, 8)
		rdb.file.Read(expireTimeByte)
		expireTime = int64(binary.LittleEndian.Uint32(expireTimeByte)) * 1000
		_, _ = rdb.ReadByte()
	case OP_EXPIRETIME:
		expireTimeByte := make([]byte, 4)
		rdb.file.Read(expireTimeByte)
		expireTime = int64(binary.LittleEndian.Uint32(expireTimeByte)) * 1000
		_, _ = rdb.ReadByte()
	}
	if expireTime > 0 && expireTime < time.Now().UnixMilli() {
		rdb.DiscardKeyValue()
		return
	}
	key := rdb.ReadRDBString()
	value := rdb.ReadRDBString()
	rdb.data = append(rdb.data, RDBData{key, value, expireTime})
}

func (rdb *RDB) DiscardKeyValue() {
	_ = rdb.ReadRDBString()
	_ = rdb.ReadRDBString()
}

func (rdb *RDB) ResizeDB() {
	hashTableSize := rdb.ReadLengthEncoded()
	expireHashTableSize := rdb.ReadLengthEncoded()
	rdb.hashTableSize = hashTableSize
	rdb.expireHashTableSize = expireHashTableSize
	for i := 0; i < hashTableSize; i++ {
		rdb.ReadKeyVal()
	}
}


func (rdb *RDB) Aux() {
	rdb.ReadRDBString()
	rdb.ReadRDBString()
}

func (rdb *RDB) CloseRDBFile() {
	rdb.file.Close()
}

func (rdb *RDB) ReadRDBString() string {
	length := rdb.ReadLengthEncoded()
	str := make([]byte, length)
	rdb.file.Read(str)
	return string(str)
}

func (rdb *RDB) ReadLengthEncoded() int {
	lengthByte := make([]byte, 1)
	rdb.file.Read(lengthByte)
	switch lengthByte[0] >> 6 {
	case 0b00:
		return int(lengthByte[0] & 0b00111111)
	case 0b01:
		rest := make([]byte, 1)
		rdb.file.Read(rest)
		return int(binary.LittleEndian.Uint16([]byte{lengthByte[0] & 0b00111111, rest[0]}))
	case 0b10:
		rest := make([]byte, 4)
		rdb.file.Read(rest)
		return int(binary.LittleEndian.Uint32(rest))
	default:
		return int(binary.LittleEndian.Uint16([]byte{lengthByte[0] & 0b00111111, 0}))
	}
}