package kafka

import (
	"encoding/binary"
	"sync"
)

// LengthField implements the PushEncoder and PushDecoder interfaces for calculating 4-byte lengths.
type lengthField struct {
	startOffset int
	length      int32
}

var lengthFieldPool = sync.Pool{}

func acquireLengthField() *lengthField {
	val := lengthFieldPool.Get()
	if val != nil {
		return val.(*lengthField)
	}
	return &lengthField{}
}

func releaseLengthField(m *lengthField) {
	lengthFieldPool.Put(m)
}

func (l *lengthField) Decode(pd PacketDecoder) error {
	var err error
	l.length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if l.length > int32(pd.remaining()) {
		return ErrInsufficientData
	}
	return nil
}

func (l *lengthField) saveOffset(in int) {
	l.startOffset = in
}

func (l *lengthField) reserveLength() int {
	return 4
}

func (l *lengthField) check(curOffset int, buf []byte) error {
	if int32(curOffset-l.startOffset-4) != l.length {
		return PacketDecodingError{"length field invalid"}
	}

	return nil
}

type varintLengthField struct {
	startOffset int
	length      int64
}

func (l *varintLengthField) Decode(pd PacketDecoder) error {
	var err error
	l.length, err = pd.getVarint()
	return err
}

func (l *varintLengthField) saveOffset(in int) {
	l.startOffset = in
}

func (l *varintLengthField) reserveLength() int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutVarint(tmp[:], l.length)
}

func (l *varintLengthField) check(curOffset int, _ []byte) error {
	if int64(curOffset-l.startOffset-l.reserveLength()) != l.length {
		return PacketDecodingError{"length field invalid"}
	}

	return nil
}
