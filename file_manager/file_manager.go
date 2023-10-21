package file_manager

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileManager struct {
	dbDirectory string
	blockSize   uint64
	isNew       bool
	openFiles   map[string]*os.File
	mu          sync.Mutex
}

func NewFileManager(dbDirectory string, blockSize uint64) (*FileManager, error) {
	fileManager := &FileManager{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		isNew:       false,
		openFiles:   make(map[string]*os.File),
	}

	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		// 目录不存在则生成
		fileManager.isNew = true
		err := os.Mkdir(dbDirectory, os.ModeDir)
		if err != nil {
			return nil, err
		}
	} else {
		// 目录存在，删除临时文件
		err := filepath.Walk(dbDirectory, func(path string, info os.FileInfo, err error) error {
			mode := info.Mode()
			if mode.IsRegular() {
				name := info.Name()
				if strings.HasPrefix(name, "temp") {
					// TODO: 可能出现问题
					os.Remove(filepath.Join(path, name))
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return fileManager, nil
}

func (f *FileManager) getFile(fileName string) (*os.File, error) {
	path := filepath.Join(f.dbDirectory, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	f.openFiles[fileName] = file
	return file, nil
}

func (f *FileManager) Read(blk *BlockId, p *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.getFile(blk.fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count, err := file.ReadAt(p.contents(), int64(blk.blkName*f.blockSize))
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (f *FileManager) Write(blk *BlockId, p *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.getFile(blk.fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count, err := file.WriteAt(p.contents(), int64(blk.blkName*f.blockSize))
	if err != nil {
		return 0, err
	}

	return count, nil
}

// Size 返回的是包含多少个区块
func (f *FileManager) Size(fileName string) (uint64, error) {
	file, err := f.getFile(fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return uint64(fi.Size()) / f.blockSize, nil
}

func (f *FileManager) Append(fileName string) (BlockId, error) {
	newBlockNum, err := f.Size(fileName)
	if err != nil {
		return BlockId{}, err
	}

	blk := NewBlockId(fileName, newBlockNum)
	file, err := f.getFile(blk.fileName)
	if err != nil {
		return BlockId{}, err
	}
	defer file.Close()

	b := make([]byte, f.blockSize)
	_, err = file.WriteAt(b, int64(blk.Number()*f.blockSize))
	if err != nil {
		return BlockId{}, err
	}
	return *blk, nil
}

func (f *FileManager) BlockSize() uint64 {
	return f.blockSize
}
