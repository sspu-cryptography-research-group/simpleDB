package log_manager

import (
	fm "simpleDB/file_manager"
	"sync"
)

const (
	UINT64_LEN = 8
)

type LogManager struct {
	fileManager  *fm.FileManager
	logFile      string      // 日志的名称
	logPage      *fm.Page    // 存储日志的缓存区
	currentBlk   *fm.BlockId // 日志当前写入的区块号
	lastestLsn   uint64      // 当前最新的日志编号
	lastSavedLsg uint64      // 上一次写入磁盘的日志编号
	mu           sync.Mutex
}

func (l *LogManager) appendNewBlock() (*fm.BlockId, error) {
	blk, err := l.fileManager.Append(l.logFile)
	if err != nil {
		return nil, err
	}

	// 设置能够写入的偏移量
	l.logPage.SetInt(0, uint64(l.fileManager.BlockSize()))
	_, err = l.fileManager.Write(&blk, l.logPage)
	if err != nil {
		return nil, err
	}

	return &blk, nil
}

func NewLogManager(fileManager *fm.FileManager, logFile string) (*LogManager, error) {
	logMgr := LogManager{
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      fm.NewPageBySize(fileManager.BlockSize()),
		lastestLsn:   0,
		lastSavedLsg: 0,
	}

	logSize, err := fileManager.Size(logFile)
	if err != nil {
		return nil, err
	}

	if logSize == 0 {
		// 文件为空，给文件创建一个新区块
		blk, err := logMgr.appendNewBlock()
		if err != nil {
			return nil, err
		}
		logMgr.currentBlk = blk
	} else {
		// 文件存在，把末尾日志读入内存。如果当前区块还有空间，新的日志就写入当前区块
		logMgr.currentBlk = fm.NewBlockId(logFile, logSize-1)
		_, err := fileManager.Read(logMgr.currentBlk, logMgr.logPage)
		if err != nil {
			return nil, err
		}
	}
	return &logMgr, nil
}

// FlushByLSN : LSN -> log sequence number
func (l *LogManager) FlushByLSN(lsn uint64) error {
	if lsn > l.lastSavedLsg {
		err := l.Flush()
		if err != nil {
			return err
		}
		l.lastSavedLsg = lsn
	}
	return nil
}

func (l *LogManager) Flush() error {
	_, err := l.fileManager.Write(l.currentBlk, l.logPage)
	if err != nil {
		return err
	}
	return nil
}

// Append 返回的是写入的日志的编号
func (l *LogManager) Append(logRecord []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	boundary := l.logPage.GetInt(0)
	recordSize := len(logRecord)
	bytesNeed := recordSize + UINT64_LEN
	var err error
	if int(boundary)-bytesNeed < int(UINT64_LEN) {
		// 没有足够的空间，现将当前缓冲写入磁盘
		err = l.Flush()
		if err != nil {
			return 0, err
		}

		// 分配新的空间用于写入日志
		l.currentBlk, err = l.appendNewBlock()
		if err != nil {
			return 0, err
		}

		boundary = l.logPage.GetInt(0)
	}

	recordPos := boundary - uint64(bytesNeed)
	l.logPage.SetBytes(recordPos, logRecord)
	l.logPage.SetInt(0, recordPos)
	l.lastestLsn += 1

	return l.lastestLsn, nil
}

func (l *LogManager) Iterator() *LogIterator {
	err := l.Flush()
	if err != nil {
		panic(err)
	}
	return NewLogIterator(l.fileManager, l.currentBlk)
}
