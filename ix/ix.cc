
#include "ix.h"
#include "../util/errcodes.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
    : _pfm(*(PagedFileManager::instance()))
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::newPage(FileHandle &fileHandle,
                         const PageNum pageNum, 
                         const bool isLeaf,
                         const PageNum num)
{
    IndexPageFooter footer;
    footer.pageNum = pageNum;
    footer.freeMemoryOffset = 0;
    footer.numSlots = 0;
    footer.isLeaf = isLeaf;
    footer.nextLeaf = 0;
    footer.firstRID.pageNum = 0;
    footer.firstRID.slotNum = 0;

    unsigned char buffer[PAGE_SIZE] = {0};
    memcpy(buffer + PAGE_SIZE - sizeof(IndexPageFooter), &footer, sizeof(IndexPageFooter));

    return fileHandle.appendPage(buffer);
}

RC IndexManager::createFile(const string &fileName)
{
    RC ret = _pfm.createFile(fileName);
    RETURN_ON_ERR(ret);

    IndexFileHeader ixfh;
    ixfh.root = 1;

    FileHandle fileHandle;
    _pfm.openFile(fileName, fileHandle);
    

    // Write reserved page with location root (initially pageNum 1)
    // Cache root page location
    _rootMap[fileName] = 1;
    unsigned char buffer[PAGE_SIZE] = {0};
    memcpy(buffer, &ixfh, sizeof(IndexFileHeader));
    fileHandle.appendPage(buffer);
    
    // Write the page footer for the root
    newPage(fileHandle, 1, true, 0);

    _pfm.closeFile(fileHandle);

	return err::OK;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pfm.destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return _pfm.openFile(fileName, fileHandle);
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
    return _pfm.closeFile(fileHandle);
}

IndexPageFooter* IndexManager::getIXFooter(const void* buffer) 
{
    return (IndexPageFooter*)((char*)buffer + PAGE_SIZE - sizeof(IndexPageFooter));
}
IndexSlot* IndexManager::getIXSlot(const int slotNum, 
                                   const void* buffer)
{
    unsigned offset = PAGE_SIZE;
    offset -= sizeof(IndexPageFooter);
    offset -= ((slotNum + 1) * sizeof(PageIndexEntry));
    return (IndexSlot*)((char*)buffer + offset);
}

RC IndexManager::loadIXRecord(unsigned size, unsigned offset, void* buffer, IndexRecord &record) {

    return -1;
}

RC IndexManager::loadRootPage(FileHandle &fileHandle, void* buffer)
{
    unsigned char reservedPage[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(0, reservedPage);
    RETURN_ON_ERR(ret);
    // Recover file header to get root page location
    IndexFileHeader ixfh;
    memcpy(&ixfh, reservedPage, sizeof(IndexFileHeader));
    return fileHandle.readPage(ixfh.root, buffer);
}

RC IndexManager::findLeafPage(FileHandle &fileHandle, KeyData &key, PageNum &pageNum)
{
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = loadRootPage(fileHandle, buffer);
    RETURN_ON_ERR(ret);
    IndexPageFooter* footer = getIXFooter(buffer);
    IndexSlot* slot = getIXSlot(footer->firstRID.slotNum, buffer);
    IndexRecord record;
    PageNum child;
    ret = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, record);
    RETURN_ON_ERR(ret);

    while (not footer->isLeaf) {
        // At beginning of each iteration of this loop, record is the FIRST
        // record on the page w.r.t. the key's ordering

        // Check if key belongs in a leftmost descendent of current page
        if (key.compare(record.key) < 0) {
            ret = fileHandle.readPage(footer->child, buffer);
            RETURN_ON_ERR(ret);
            slot = getIXSlot(footer->firstRID.slotNum, buffer);
            ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, record);
            RETURN_ON_ERR(ret);
            continue;
        }

        // If not, traverse records, comparing keys
        while (key.compare(record.key) >= 0 && record.nextSlot.pageNum == footer->pageNum) {
            // Save the current child page: it will be the correct child if we break next iteration
            child = record.rid.pageNum;
            // Update record to the next one
            slot = getIXSlot(record.nextSlot.pageNum, buffer);
            ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, record);
            RETURN_ON_ERR(ret);
        }

        // This will only happen if we reach last record
        if (record.nextSlot.pageNum != footer->pageNum) 
            child = record.rid.pageNum;

        ret = fileHandle.readPage(child, buffer);
        RETURN_ON_ERR(ret);
        slot = getIXSlot(footer->firstRID.slotNum, buffer);
        ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, record);
        RETURN_ON_ERR(ret);
    }

    pageNum = footer->pageNum;
    return err::OK;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // Build entry structure to be written to the leaf page
    // Find proper leaf page to put entry
    // Find proper "previous" leaf
    // If there is room to insert, do so
    //// Make the "previous" leaf point to "new" leaf and "new" leaf point to "previous"'s "next" leaf
    // If there is not room, we need to split
	return -1;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

RC IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}

void IX_PrintError (RC rc)
{
}

int KeyData::compare(KeyData &key) {
    switch (type) {
        case TypeInt:
        case TypeReal:
            {
                if (integer < key.integer) return -1;
                else if (integer == key.integer) return 0;
                else return 1;
            }
        case TypeVarChar:
            return strcmp(varchar, key.varchar);
    }
}
