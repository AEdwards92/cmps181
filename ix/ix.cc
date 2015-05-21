
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

void IndexManager::writeIXSlot(void* buffer,
                               unsigned slotNum,
                               IndexSlot* slot)
{                                              
    unsigned offset = PAGE_SIZE;
    offset -= sizeof(IndexPageFooter);
    offset -= ((slotNum + 1) * sizeof(IndexSlot));
    memcpy((char*)buffer + offset, slot, sizeof(IndexSlot));
} 

IndexSlot* IndexManager::getIXSlot(const int slotNum, 
                                   const void* buffer)
{
    unsigned offset = PAGE_SIZE;
    offset -= sizeof(IndexPageFooter);
    offset -= ((slotNum + 1) * sizeof(IndexSlot));
    return (IndexSlot*)((char*)buffer + offset);
}

RC IndexManager::loadIXRecord(unsigned size, unsigned offset, void* buffer, AttrType type, IndexRecord &record) {
    record.key.size = size - 2*sizeof(RID);
    record.key.type = type;
    switch (type) {
        case TypeInt:
            memcpy(&(record.key.integer), (unsigned*)buffer + offset, sizeof(int));
            break;
        case TypeReal:
            memcpy(&(record.key.real), (unsigned*)buffer + offset, sizeof(float));
            break;
        case TypeVarChar:
            {
                unsigned dataLen = *((unsigned*)buffer + offset);
                memcpy(&(record.key.varchar), (unsigned*)buffer + offset + sizeof(unsigned), dataLen);
                break;
            }
    }
    memcpy(&(record.rid),  (unsigned*)buffer + offset + record.key.size, sizeof(RID));
    memcpy(&(record.nextSlot),  (unsigned*)buffer + offset + record.key.size + sizeof(RID), sizeof(RID));
    return err::OK;
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

RC IndexManager::loadKeyData(const void* data, AttrType type, KeyData& key) {
    int size = 0;
    switch (type) {
        case TypeInt:
            memcpy(&(key.integer), (unsigned*)data, sizeof(int));
            size = sizeof(int);
            break;
        case TypeReal:
            memcpy(&(key.real), (unsigned*)data, sizeof(float));
            size = sizeof(float);
            break;
        case TypeVarChar:
            {
                unsigned dataLen = *((unsigned*)data);
                memcpy(&(key.varchar), (unsigned*)data + sizeof(unsigned), dataLen);
                size = sizeof(unsigned) * (dataLen + 1);
            }
    }
    key.size = size;
    key.type = type;
    return err::OK;
}

RC IndexManager::loadLeafPage(FileHandle &fileHandle, KeyData &key, AttrType type, vector<PageNum> parents, unsigned char* buffer)
{
    // Read in root page
    RC ret = loadRootPage(fileHandle, buffer);
    RETURN_ON_ERR(ret);

    IndexPageFooter* footer = getIXFooter(buffer);
    if (footer->isLeaf) return err::OK;

    // Load up first record on page. Since root is not a leaf, we know there must be at least one entry
    IndexSlot* slot = getIXSlot(footer->firstRID.slotNum, buffer);
    IndexRecord record;
    PageNum child;
    ret = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
    RETURN_ON_ERR(ret);

    // traverse tree
    while (not footer->isLeaf) {
        // At beginning of each iteration of this loop, record is the FIRST
        // record on the page w.r.t. the key's ordering
        parents.push_back(footer->pageNum);

        // Check if key belongs in a leftmost descendent of current page
        if (key.compare(record.key) < 0) {
            ret = fileHandle.readPage(footer->child, buffer);
            RETURN_ON_ERR(ret);
            slot = getIXSlot(footer->firstRID.slotNum, buffer);
            ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
            RETURN_ON_ERR(ret);
            continue;
        }

        // If not, traverse records, comparing keys
        while (key.compare(record.key) >= 0 && record.nextSlot.pageNum == footer->pageNum) {
            // Save the current child page: it will be the correct child if we break next iteration
            child = record.rid.pageNum;
            // Update record to the next one
            slot = getIXSlot(record.nextSlot.pageNum, buffer);
            ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
            RETURN_ON_ERR(ret);
        }

        // This will only happen if we reach last record
        if (record.nextSlot.pageNum != footer->pageNum) 
            child = record.rid.pageNum;

        ret = fileHandle.readPage(child, buffer);
        RETURN_ON_ERR(ret);
        slot = getIXSlot(footer->firstRID.slotNum, buffer);
        ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
        RETURN_ON_ERR(ret);
    }
    return err::OK;
}

bool IndexManager::needsToSplit(KeyData& key, IndexPageFooter* footer) {
    // Returns true if and only if we can insert key into current page
    unsigned space = PAGE_SIZE - sizeof(IndexPageFooter) 
                               - footer->numSlots*sizeof(IndexSlot) 
                               - footer->freeMemoryOffset;
    return space < key.size + 2 * sizeof(RID);
}

RC IndexManager::splitHandler(FileHandle& fileHandle, vector<PageNum> parents, unsigned char* buffer)
{
    return -1;
}

RC IndexManager::writeRecordToBuffer(KeyData& key, const RID& rid, const RID& nextSlot, AttrType type, IndexPageFooter* footer, unsigned char* buffer)
{
    // build new record and new slot
    IndexSlot newSlot;
    newSlot.type = ALIVE;
    newSlot.recordOffset = footer->freeMemoryOffset;
    newSlot.recordSize = 2 * sizeof(RID) + key.size;

    // Write key data to page at beginning of free memory
    switch (type) {
        case TypeInt:
            memcpy(buffer + footer->freeMemoryOffset, &(key.integer), sizeof(int));
            break;
        case TypeReal:
            memcpy(buffer + footer->freeMemoryOffset, &(key.integer), sizeof(float));
            break;
        case TypeVarChar:
            memcpy(buffer + footer->freeMemoryOffset, &(key.varchar), key.size);
    }

    // Write RIDs
    memcpy(buffer + footer->freeMemoryOffset + key.size, &rid, sizeof(RID));
    memcpy(buffer + footer->freeMemoryOffset + key.size + sizeof(RID), &nextSlot, sizeof(RID));

    // Update freeMemoryOffset
    footer->freeMemoryOffset += key.size + 2 * sizeof(RID);
    return err::OK;
}

RC IndexManager::insertInOrder(KeyData& key, AttrType type, const RID &rid, unsigned char* buffer) 
{
    // recover footer from page, locate first slot w.r.t. key order
    IndexPageFooter* footer = getIXFooter(buffer);
    IndexSlot* currSlot = getIXSlot(footer->firstRID.slotNum, buffer);
    IndexSlot* prevSlot;
    IndexRecord record;
    RID currRID = footer->firstRID;
    RID prevRID;
    RC ret = loadIXRecord(currSlot->recordSize, currSlot->recordOffset, buffer, type, record);
    RETURN_ON_ERR(ret);

    IndexSlot newSlot;
    newSlot.type = ALIVE;
    newSlot.recordSize = key.size + 2 * sizeof(RID);
    newSlot.recordOffset = footer->freeMemoryOffset;

    RID newRID;
    newRID.pageNum = footer->pageNum;
    newRID.slotNum = footer->numSlots;
    // check if key should be new first record
    if (footer->numSlots == 0 || key.compare(record.key) < 0) {
        // write record at start of free memory
        writeRecordToBuffer(key, rid, currRID, type, footer, buffer); 
        writeIXSlot(buffer, newRID.slotNum, &newSlot);
        footer->firstRID = newRID;
    } else {
        // iterate through linked list, skipping dead entries
        while (key.compare(record.key) >= 0) {
            // Check if we are at the last record
            if (record.nextSlot.pageNum != footer->pageNum) {
                RID blankRID;
                blankRID.pageNum = 0;
                blankRID.slotNum = 0;
                // write record at start of free memory
                writeRecordToBuffer(key, rid, blankRID, type, footer, buffer); 
                // update "previous" entry to point to new record
                memcpy(buffer + currSlot->recordOffset + currSlot->recordSize - sizeof(RID), &newRID, sizeof(RID));
                // write new slot
                writeIXSlot(buffer, newRID.slotNum, &newSlot);
                footer->numSlots++;
                return err::OK;
            }
            do {
                prevSlot = currSlot;
                prevRID = currRID;
                currSlot = getIXSlot(currRID.pageNum, buffer);
                currRID = record.nextSlot;
                ret  = loadIXRecord(currSlot->recordSize, currSlot->recordOffset, buffer, type, record);
                RETURN_ON_ERR(ret);
            } while (currSlot->type == DEAD);
        } 
        // write record at start of free memory
        writeRecordToBuffer(key, rid, currRID, type, footer, buffer); 
        // update "previous" entry to point to new record
        memcpy(buffer + prevSlot->recordOffset + prevSlot->recordSize - sizeof(RID), &newRID, sizeof(RID));
        // write new slot
        writeIXSlot(buffer, newRID.slotNum, &newSlot);
    }

    // note: writeRecordToBuffer already updates footer->freeMemoryOffset
    //       still need to update number of slots
    footer->numSlots++;
    return err::OK;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // Build key structure
    KeyData key_struct;
    RC ret = loadKeyData(key, attribute.type, key_struct);
    RETURN_ON_ERR(ret);

    unsigned char buffer[PAGE_SIZE] = {0};
    vector<PageNum> parents;
    ret = loadLeafPage(fileHandle, key_struct, attribute.type, parents, buffer);
    RETURN_ON_ERR(ret);

    // Check if leaf page must be split
    // If not, insert the entry and finish.
    // Otherwise, perform cascading splits and call insertEntry again

    IndexPageFooter* footer = getIXFooter(buffer);

    // Now find the "previous" record for the current entry
    if (needsToSplit(key_struct, footer)) {
        ret = splitHandler(fileHandle, parents, buffer);
        RETURN_ON_ERR(ret);
    }
    // Otherwise we can just perform insertion
    ret = insertInOrder(key_struct, attribute.type, rid, buffer);
    RETURN_ON_ERR(ret);

    return fileHandle.writePage(footer->pageNum, buffer);
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
