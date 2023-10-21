package file_manager

import "encoding/binary"

type Page struct {
	buffer []byte
}

func NewPageBySize(blockSize uint64) *Page {
	bytes := make([]byte, blockSize)
	return &Page{
		buffer: bytes,
	}
}

func NewPageByBytes(bytes []byte) *Page {
	return &Page{
		buffer: bytes,
	}
}

func (p *Page) GetInt(offset uint64) uint64 {
	num := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	return num
}

func uint64ToByteArray(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

// SetInt 由调用方考虑buffer越界大小的问题
func (p *Page) SetInt(offset uint64, val uint64) {
	b := uint64ToByteArray(val)
	copy(p.buffer[offset:], b)
}

func (p *Page) GetBytes(offset uint64) []byte {
	length := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	newBuf := make([]byte, length)
	copy(newBuf, p.buffer[offset+8:])
	return newBuf
}

func (p *Page) SetBytes(offset uint64, b []byte) {
	length := uint64(len(b))
	lenBuf := uint64ToByteArray(length)
	copy(p.buffer[offset:], lenBuf)
	copy(p.buffer[offset+8:], b)
}

func (p *Page) GetString(offset uint64) string {
	strBytes := p.GetBytes(offset)
	return string(strBytes)
}

func (p *Page) SetString(offset uint64, s string) {
	strBytes := []byte(s)
	p.SetBytes(offset, strBytes)
}

func (p *Page) MaxLengthForString(s string) uint64 {
	bs := []byte(s)
	uint64Size := 8
	return uint64(uint64Size + len(bs))
}

func (p *Page) contents() []byte {
	return p.buffer
}
