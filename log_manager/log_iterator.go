package log_manager

import fm "simpleDB/file_manager"

type LogIterator struct {
	fileManager *fm.FileManager
	blk         *fm.BlockId
	p           *fm.Page
	currentPos  uint64
	boundary    uint64
}

func NewLogIterator(file *fm.FileManager, blk *fm.BlockId) *LogIterator {
	it := LogIterator{
		fileManager: file,
		blk:         blk,
	}

	it.p = fm.NewPageBySize(file.BlockSize())
	err := it.moveToBlock(blk)
	if err != nil {
		return nil
	}

	return &it
}

func (it *LogIterator) moveToBlock(blk *fm.BlockId) error {
	_, err := it.fileManager.Read(blk, it.p)
	if err != nil {
		return err
	}
	it.boundary = it.p.GetInt(0)
	it.currentPos = it.boundary
	return nil
}

func (it *LogIterator) Next() []byte {
	if it.currentPos == it.fileManager.BlockSize() {
		it.blk = fm.NewBlockId(it.blk.FileName(), it.blk.Number()-1)
		// TODO
		err := it.moveToBlock(it.blk)
		if err != nil {
			panic(err)
		}
	}
	record := it.p.GetBytes(it.currentPos)
	it.currentPos += UINT64_LEN + uint64(len(record))
	return record
}

func (it *LogIterator) HashNext() bool {
	return it.currentPos < it.fileManager.BlockSize() || it.blk.Number() > 0
}
