package file_manager

import (
	"crypto/sha256"
	"fmt"
)

type BlockId struct {
	fileName string // 对应的二进制文件名
	blkName  uint64 // 二进制文件中对应的区块标号
}

func NewBlockId(fileName string, blkName uint64) *BlockId {
	return &BlockId{
		fileName: fileName,
		blkName:  blkName,
	}
}

func (b *BlockId) FileName() string {
	return b.fileName
}

func (b *BlockId) Number() uint64 {
	return b.blkName
}

func (b *BlockId) Equal(other *BlockId) bool {
	return b.fileName == other.fileName && b.blkName == other.blkName
}

func asSha256(o interface{}) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", o)))

	return fmt.Sprintf("%x", h.Sum(nil))
}

func (b *BlockId) HashCode() string {
	return asSha256(*b)
}
