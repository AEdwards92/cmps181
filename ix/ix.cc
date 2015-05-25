
#include "ix.h"
#include "../util/errcodes.h"
#include <string>

// copy constructor
IndexRecord::IndexRecord(const IndexRecord& that) {
    rid = that.rid;
    nextSlot = that.nextSlot;
    switch (that.key.type) {
        case TypeInt:
            key.integer = that.key.integer;
            break;
        case TypeReal:
            key.real = that.key.real;
            break;
        case TypeVarChar:
            memcpy(&(key.varchar), &(that.key.varchar), that.key.size);
    }
}

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
                         const bool isLeaf,
                         const PageNum num)
{
    IndexPageFooter footer;
    footer.pageNum = fileHandle.getNumberOfPages();
    footer.freeMemoryOffset = 0;
    footer.numSlots = 0;
    footer.isLeaf = isLeaf;
    if (isLeaf)
        footer.nextLeaf = num;
    else 
        footer.child = num;
    footer.firstRID.pageNum = 0;
    footer.firstRID.slotNum = 0;

    unsigned char buffer[PAGE_SIZE] = {0};
    memcpy(buffer + PAGE_SIZE - sizeof(IndexPageFooter), &footer, sizeof(IndexPageFooter));

    return fileHandle.appendPage(buffer);
}

RC IndexManager::initPage(FileHandle &fileHandle,
                          const PageNum pageNum, 
                          const bool isLeaf,
                          const PageNum num,
                          unsigned char* buffer)
{
    IndexPageFooter footer;
    footer.pageNum = pageNum;
    footer.freeMemoryOffset = 0;
    footer.numSlots = 0;
    footer.isLeaf = isLeaf;
    if (isLeaf)
        footer.nextLeaf = num;
    else 
        footer.child = num;
    footer.firstRID.pageNum = 0;
    footer.firstRID.slotNum = 0;
    memcpy(buffer + PAGE_SIZE - sizeof(IndexPageFooter), &footer, sizeof(IndexPageFooter));
    return err::OK;
}

RC IndexManager::newRootPage(FileHandle& fileHandle,
                             const PageNum num,
                             AttrType type,
                             IndexRecord& divider)
{
    IndexPageFooter footer;
    footer.pageNum = fileHandle.getNumberOfPages();
    footer.freeMemoryOffset = 0;
    footer.numSlots = 0;
    footer.isLeaf = false;

    footer.child = num;

    footer.firstRID.pageNum = 0;
    footer.firstRID.slotNum = 0;

    unsigned char buffer[PAGE_SIZE] = {0};
    memcpy(buffer + PAGE_SIZE - sizeof(IndexPageFooter), &footer, sizeof(IndexPageFooter));

    RC ret = insertInOrder(divider.key, type, divider.rid, buffer);
    RETURN_ON_ERR(ret);

    ret = fileHandle.appendPage(buffer);
    RETURN_ON_ERR(ret);

    // Recover file header to get root page location
    unsigned char reservedPage[PAGE_SIZE] = {0};
    ret = fileHandle.readPage(0, reservedPage);
    RETURN_ON_ERR(ret);

    // update file header, and cache root page num
    IndexFileHeader* ixfh = (IndexFileHeader*)reservedPage;
    ixfh->root = footer.pageNum;
    _rootMap[fileHandle.getFileName()] = footer.pageNum;

    return fileHandle.writePage(0, reservedPage);
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
    newPage(fileHandle, true, 0);

    _pfm.closeFile(fileHandle);

	return err::OK;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pfm.destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, 
                          FileHandle &fileHandle)
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

RC IndexManager::loadIXRecord(unsigned size, 
                              unsigned offset, 
                              unsigned char* buffer, 
                              AttrType type, 
                              IndexRecord &record) {
    record.key.size = size - 2*sizeof(RID);
    record.key.type = type;
    switch (type) {
        case TypeInt:
            memcpy(&(record.key.integer), buffer + offset, sizeof(int));
            break;
        case TypeReal:
            memcpy(&(record.key.real), buffer + offset, sizeof(float));
            break;
        case TypeVarChar:
            {
                unsigned dataLen = *(buffer + offset);
                memcpy(&(record.key.varchar), buffer + offset + sizeof(unsigned), dataLen);
                break;
            }
    }
    memcpy(&(record.rid), buffer + offset + record.key.size, sizeof(RID));
    memcpy(&(record.nextSlot), buffer + offset + record.key.size + sizeof(RID), sizeof(RID));
    return err::OK;
}

PageNum IndexManager::rootPageNum(FileHandle &fileHandle)
{
    string fileName = fileHandle.getFileName();
    if (_rootMap.find(fileName) != _rootMap.end()) 
        return _rootMap[fileName];

    unsigned char reservedPage[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(0, reservedPage);
    RETURN_ON_ERR(ret);

    // Recover file header to get root page location
    IndexFileHeader* ixfh = (IndexFileHeader*)reservedPage;
    
    _rootMap[fileName] = ixfh->root;
    return ixfh->root;
}

RC IndexManager::loadRootPage(FileHandle &fileHandle, 
                              void* buffer)
{
    return fileHandle.readPage(rootPageNum(fileHandle), buffer);
}

RC IndexManager::loadKeyData(const void* data, 
                             AttrType type, 
                             KeyData& key) {
    unsigned size = 0;
    switch (type) {
        case TypeInt:
            memcpy(&(key.integer), data, sizeof(int));
            size = sizeof(int);
            break;
        case TypeReal:
            memcpy(&(key.real), data, sizeof(float));
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

RC IndexManager::loadLeafPage(FileHandle &fileHandle, 
                              KeyData &key, 
                              AttrType type, 
                              vector<PageNum>& parents, 
                              unsigned char* buffer)
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
            slot = getIXSlot(record.nextSlot.slotNum, buffer);
            ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
            RETURN_ON_ERR(ret);
        }

        // This will only happen if we reach last record
        if (key.compare(record.key) >= 0 && record.nextSlot.pageNum != footer->pageNum) 
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

RC IndexManager::splitHandler(FileHandle& fileHandle, 
                              vector<PageNum>& parents,
                              const AttrType type, 
                              KeyData key, 
                              RID rid,
                              unsigned char* buffer)
{
    // buffer has page to be split
    // parents has successive ancestors of leaf page
    // bring up two new buffers
    unsigned char lowerHalf[PAGE_SIZE] = {0};
    unsigned char upperHalf[PAGE_SIZE] = {0};

    IndexPageFooter* footer = getIXFooter(buffer);

    PageNum upperHalfPageNum = fileHandle.getNumberOfPages();

    if (footer->isLeaf) { 
        // Now "nextLeaf" is the upper half
        initPage(fileHandle, footer->pageNum, footer->isLeaf, upperHalfPageNum, lowerHalf);
    } else {
        // Child is the same as before
        initPage(fileHandle, footer->pageNum, footer->isLeaf, footer->child, lowerHalf);
    }

    // begin writing records to lowerHalf until we have written more than PAGE_SIZE/2
    IndexSlot* slot = getIXSlot(footer->firstRID.slotNum, buffer);
    IndexRecord record;
    RC ret = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
    RETURN_ON_ERR(ret);
    
    bool halfFull = false;
    const unsigned totalSpace = PAGE_SIZE - sizeof(IndexPageFooter);
    unsigned availableSpace = totalSpace;
    while (not halfFull) {
        insertInOrder(record.key, type, record.rid, lowerHalf);
        availableSpace -= sizeof(IndexSlot);
        availableSpace -= slot->recordSize;
        halfFull = 2 * availableSpace < totalSpace; 
        slot = getIXSlot(record.nextSlot.slotNum, buffer);
        ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
        RETURN_ON_ERR(ret);
    }

    // Cache the middle value 
    IndexRecord divider;
    divider.key = record.key;
    divider.rid.pageNum = fileHandle.getNumberOfPages();

    // Now we copy remaining entries to upperHalf

    // We only copy the divider if this is a leafPage...
    if (not footer->isLeaf) {
        // ... otherwise we load next record, and set new interior node's leftmost child
        // to the child of the middle record
        initPage(fileHandle, upperHalfPageNum, footer->isLeaf, record.rid.pageNum, upperHalf);
        slot = getIXSlot(record.nextSlot.slotNum, buffer);
        ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
        RETURN_ON_ERR(ret);
    }else {
        initPage(fileHandle, upperHalfPageNum, footer->isLeaf, footer->nextLeaf, upperHalf);
    }

    while (true) {
        insertInOrder(record.key, type, record.rid, upperHalf);
        if (record.nextSlot.pageNum != footer->pageNum) break;
        slot = getIXSlot(record.nextSlot.slotNum, buffer);
        ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, type, record);
        RETURN_ON_ERR(ret);
    }

    // Finally insert the key
    if (key.compare(divider.key) < 0) {
        ret = insertInOrder(key, type, rid, lowerHalf);
    } else {
        ret = insertInOrder(key, type, rid, upperHalf);
    }
    RETURN_ON_ERR(ret);

    // Last thing to do: have last entry on lowerHalf point to first entry 
    // on upperHalf
    if (footer->isLeaf) {
        RID linkRID;
        linkRID.pageNum = fileHandle.getNumberOfPages();
        linkRID.slotNum = 0;
        IndexPageFooter *lowerFooter = getIXFooter(lowerHalf);
        slot = getIXSlot(lowerFooter->firstRID.slotNum, lowerHalf);
        ret  = loadIXRecord(slot->recordSize, slot->recordOffset, lowerHalf, type, record);
        while (record.nextSlot.pageNum == lowerFooter->pageNum) {
            slot = getIXSlot(record.nextSlot.slotNum, lowerHalf);
            ret  = loadIXRecord(slot->recordSize, slot->recordOffset, lowerHalf, type, record);
        }
        memcpy(lowerHalf + slot->recordOffset + slot->recordSize - sizeof(RID), &linkRID, sizeof(RID));
        RETURN_ON_ERR(ret);
    }

    // overwrite the split page
    fileHandle.writePage(footer->pageNum, lowerHalf);
    // update buffer contents 
    memcpy(buffer, lowerHalf, sizeof(PAGE_SIZE));
    // Write upperHalf to file
    fileHandle.appendPage(upperHalf);

    // we are not fininished: we need to update ancestor nodes
    unsigned char parentBuffer[PAGE_SIZE] = {0};
    // if the split page is root, then create new root page
    if (parents.empty()) {
        ret =  newRootPage(fileHandle, footer->pageNum, type, divider);
        RETURN_ON_ERR(ret);
    } else {
        // Othwersie, check if we can insert the divider into the parent
        PageNum parent = parents.back();
        parents.pop_back();
        fileHandle.readPage(parent, parentBuffer);
        IndexPageFooter* parentFooter = getIXFooter(parentBuffer);
        if (needsToSplit(divider.key, parentFooter)) {
            // Recurse
            ret = splitHandler(fileHandle, parents, type, divider.key, divider.rid, parentBuffer);
            RETURN_ON_ERR(ret);
            // Now parent buffer has lower half of its original contents
        } else {
            // Sweet, we can just insert in the parent and we are done
            ret = insertInOrder(divider.key, type, divider.rid, parentBuffer);
            RETURN_ON_ERR(ret);
            return fileHandle.writePage(parentFooter->pageNum, parentBuffer);
        }
    }

    return err::OK;
}

RC IndexManager::writeRecordToBuffer(KeyData& key, 
                                     const RID& rid, 
                                     const RID& nextSlot, 
                                     AttrType type, 
                                     IndexPageFooter* footer, 
                                     unsigned char* buffer)
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
    if (type == TypeInt && key.integer == 4946)
        cout <<"";
    
    // recover footer from page, locate first slot w.r.t. key order
    IndexPageFooter* footer = getIXFooter(buffer);
    IndexSlot* currSlot = getIXSlot(footer->firstRID.slotNum, buffer);
    IndexSlot* prevSlot;
    IndexRecord record;
    RID currRID = footer->firstRID;

    IndexSlot newSlot;
    newSlot.type = ALIVE;
    newSlot.recordSize = key.size + 2 * sizeof(RID);
    newSlot.recordOffset = footer->freeMemoryOffset;

    RID blankRID;
    blankRID.pageNum = 0; 
    blankRID.slotNum = 0;

    RID newRID;
    newRID.pageNum = footer->pageNum;
    newRID.slotNum = footer->numSlots;

    if (footer->numSlots == 0) {
        // write record at start of free memory
        writeRecordToBuffer(key, rid, blankRID, type, footer, buffer); 
        writeIXSlot(buffer, newRID.slotNum, &newSlot);
        footer->firstRID = newRID;
        footer->numSlots++;
#ifdef DEBUG
        cerr << "DEBUG: INSERTED " << key.toString() << " ON PAGE " << newRID.pageNum << " IN SLOT " << newRID.slotNum << endl;
#endif
        return err::OK;
    }

    RC ret = loadIXRecord(currSlot->recordSize, currSlot->recordOffset, buffer, type, record);
    RETURN_ON_ERR(ret);


    // check if key should be new first record
    if (key.compare(record.key) < 0) {
        // write record at start of free memory
        writeRecordToBuffer(key, rid, currRID, type, footer, buffer); 
        writeIXSlot(buffer, newRID.slotNum, &newSlot);
        footer->firstRID = newRID;
    } else {
        // iterate through linked list, skipping dead entries
        while (key.compare(record.key) >= 0) {
            do {
                // Check if we are at the last record
                if (record.nextSlot.pageNum != footer->pageNum) {
                    // write record at start of free memory
                    writeRecordToBuffer(key, rid, record.nextSlot, type, footer, buffer); 
                    // update "previous" entry to point to new record
                    memcpy(buffer + currSlot->recordOffset + currSlot->recordSize - sizeof(RID), &newRID, sizeof(RID));
                    // write new slot
                    writeIXSlot(buffer, newRID.slotNum, &newSlot);
                    footer->numSlots++;
#ifdef DEBUG
                    cerr << "DEBUG: INSERTED " << key.toString() << " ON PAGE " << newRID.pageNum << " IN SLOT " << newRID.slotNum << endl;
#endif
                    return err::OK;
                }
                prevSlot = currSlot;
                currSlot = getIXSlot(currRID.slotNum, buffer);
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
#ifdef DEBUG
    cerr << "DEBUG: INSERTED " << key.toString() << " ON PAGE " << newRID.pageNum << " IN SLOT " << newRID.slotNum << endl;
#endif

    // note: writeRecordToBuffer already updates footer->freeMemoryOffset
    //       still need to update number of slots
    footer->numSlots++;
    return err::OK;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, 
                             const Attribute &attribute, 
                             const void *key, 
                             const RID &rid)
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
    // Otherwise, perform cascading splits 

    IndexPageFooter* footer = getIXFooter(buffer);

    if (not needsToSplit(key_struct, footer)) {
        // Otherwise we can just perform insertion
        ret = insertInOrder(key_struct, attribute.type, rid, buffer);
        RETURN_ON_ERR(ret);
        return fileHandle.writePage(footer->pageNum, buffer);
    } else {
        // Pass control to split handler
        // Will perform cascade of splits and also insert necessary entries
#ifdef DEBUG
        cerr << "DEBUG: SPLITTING PAGE " << footer->pageNum << endl;
#endif
        return splitHandler(fileHandle, parents, attribute.type, key_struct, rid, buffer);
    }
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, 
                             const Attribute &attribute, 
                             const void *key, 
                             const RID &rid)
{
    // Build key structure
    KeyData key_struct;
    RC ret = loadKeyData(key, attribute.type, key_struct);
    RETURN_ON_ERR(ret);

    unsigned char buffer[PAGE_SIZE] = {0};
    vector<PageNum> parents;
    ret = loadLeafPage(fileHandle, key_struct, attribute.type, parents, buffer);
    RETURN_ON_ERR(ret);


    // Find entry on page
    // 
    IndexPageFooter* footer = getIXFooter(buffer);
    IndexSlot* slot = getIXSlot(footer->firstRID.slotNum, buffer);
    IndexRecord record;
    ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, attribute.type, record);

    // walk list of entries,  ...
    bool ridsMatch = record.rid.pageNum == rid.pageNum and record.rid.slotNum == rid.slotNum;
    while (key_struct.compare(record.key) > 0) { 
        if (record.nextSlot.pageNum != footer->pageNum) 
            return err::RECORD_DOES_NOT_EXIST;
 
       // skip the dead
       do {
           slot = getIXSlot(record.nextSlot.slotNum, buffer);
           ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, attribute.type, record);
           RETURN_ON_ERR(ret);
        } while (slot->type == DEAD and (key_struct.compare(record.key) > 0));

        ridsMatch = record.rid.pageNum == rid.pageNum and record.rid.slotNum == rid.slotNum;
    }
    
    // Ensure that RID's also match 
    // If not, keep walking, but if the equality condition is broken, return an error
    while (not ridsMatch) {
        slot = getIXSlot(record.nextSlot.slotNum, buffer);
        ret  = loadIXRecord(slot->recordSize, slot->recordOffset, buffer, attribute.type, record);
        ridsMatch = record.rid.pageNum == rid.pageNum and record.rid.slotNum == rid.slotNum;
        if (key_struct.compare(record.key) != 0)
            return err::RECORD_DOES_NOT_EXIST;
        RETURN_ON_ERR(ret);
    }

    if (key_struct.compare(record.key) != 0 and
        (not (record.rid.pageNum == rid.pageNum && record.rid.slotNum != rid.slotNum))) {
        return err::RECORD_DOES_NOT_EXIST;
    }


    // othwerwise, slot refers to matching key
    // check that RID matches as well

    switch (slot->type)
    {
        case ALIVE:
            slot->type = DEAD;
            return fileHandle.writePage(footer->pageNum, buffer);
        case DEAD:
            return err::RECORD_DELETED;
        default:
            return err::FILE_CORRUPT;
    }
}

RC IndexManager::scan(FileHandle &fileHandle,
                      const Attribute &attribute,
                      const void      *lowKey,
                      const void      *highKey,
                      bool			lowKeyInclusive,
                      bool        	highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator)
{
    return ix_ScanIterator.init(fileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive); 
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}



RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (_eof)
        return IX_EOF;

    memcpy(&rid, &_nextRecord.rid, sizeof(RID));
    switch (_nextRecord.key.type) {
        case TypeInt:
            memcpy(key, &(_nextRecord.key.integer), sizeof(int));
            break;
        case TypeReal:
            memcpy(key, &(_nextRecord.key.real), sizeof(float));
            break;
        case TypeVarChar:
            memcpy(key, &(_nextRecord.key.varchar), _nextRecord.key.size);
    }
    RC ret = loadNextRecord();

    if (ret == IX_EOF)
        _eof = true;

    if ((not _highInclusive) and _highKey.compare(_nextRecord.key) <= 0) {
        _eof = true;
    } else if (_highKey.compare(_nextRecord.key) < 0) {
        _eof = true;
    }

    return err::OK;
}

RC IX_ScanIterator::init(FileHandle &fileHandle,
                         const Attribute &attribute,
                         const void      *lowKey,
                         const void      *highKey,
                         bool			lowKeyInclusive,
                         bool        	highKeyInclusive) 
{
    _ixfm = IndexManager::instance();
    _fileHandle = fileHandle;
    _type = attribute.type;
    _lowInclusive = lowKeyInclusive;
    _footer = NULL;
    _highInclusive = highKeyInclusive;
    _eof = false;
    RC ret;

    if (lowKey == NULL)  {
        ret = loadLowestRecord();
        RETURN_ON_ERR(ret);
        _lowKey = _nextRecord.key;
    } else
        _ixfm->loadKeyData(lowKey, attribute.type, _lowKey);

    if (highKey == NULL) {
        ret = loadHighestRecord();
        RETURN_ON_ERR(ret);
        _highKey = _highestRecord.key;
    } else
        _ixfm->loadKeyData(highKey, attribute.type, _highKey);

    loadFirstRecord();
    return err::OK;
}

RC IX_ScanIterator::loadLowestRecord()
{
    RC ret = _ixfm->loadRootPage(_fileHandle, _buffer);
    RETURN_ON_ERR(ret);
    _footer = _ixfm->getIXFooter(_buffer);
    while (not _footer->isLeaf) {
        ret = _fileHandle.readPage(_footer->child, _buffer);
        RETURN_ON_ERR(ret);
    }
    _nextSlot = _ixfm->getIXSlot(_footer->firstRID.slotNum, _buffer);
    return _ixfm->loadIXRecord(_nextSlot->recordSize, _nextSlot->recordOffset, _buffer, _type, _nextRecord);
}

RC IX_ScanIterator::loadHighestRecord()
{
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = _ixfm->loadRootPage(_fileHandle, buffer);
    RETURN_ON_ERR(ret);
    IndexPageFooter* footer = _ixfm->getIXFooter(buffer);
    IndexSlot* slot = _ixfm->getIXSlot(footer->firstRID.slotNum, buffer);
    IndexRecord record;
    ret = _ixfm->loadIXRecord(slot->recordSize, slot->recordOffset, buffer, _type, record);
    while (not footer->isLeaf) {
        while (record.nextSlot.pageNum == footer->pageNum) {
            slot = _ixfm->getIXSlot(record.nextSlot.slotNum, buffer);
            ret = _ixfm->loadIXRecord(slot->recordSize, slot->recordOffset, buffer, _type, record);
            RETURN_ON_ERR(ret);
        }
        // read rightmost child
        ret = _fileHandle.readPage(record.rid.pageNum, buffer);
        slot =  _ixfm->getIXSlot(footer->firstRID.slotNum, buffer);
        ret = _ixfm->loadIXRecord(slot->recordSize, slot->recordOffset, buffer, _type, record);
        RETURN_ON_ERR(ret);
    }
    while (record.nextSlot.pageNum == footer->pageNum) {
        slot = _ixfm->getIXSlot(record.nextSlot.slotNum, buffer);
        ret = _ixfm->loadIXRecord(slot->recordSize, slot->recordOffset, buffer, _type, record);
        RETURN_ON_ERR(ret);
    }
    _highestRecord = record;
    return err::OK;
}

RC IX_ScanIterator::loadFirstRecord()
{
    // Ensure that range is logically non-empty
    if (_lowKey.compare(_highKey) > 0) {
        _eof = true;
        return err::OK;
    }

    vector<PageNum> parents;
    
    RC ret = _ixfm->loadLeafPage(_fileHandle, _lowKey, _type, parents, _buffer);
    RETURN_ON_ERR(ret);

    _footer = _ixfm->getIXFooter(_buffer);
    _nextSlot = _ixfm->getIXSlot(_footer->firstRID.slotNum, _buffer);
    ret  = _ixfm->loadIXRecord(_nextSlot->recordSize, _nextSlot->recordOffset, _buffer, _type, _nextRecord);
    RETURN_ON_ERR(ret);

    // Find the first ALIVE entry greater than or equal to _lowKey
    while (_nextSlot->type == DEAD || _lowKey.compare(_nextRecord.key) > 0) {
        if (_nextRecord.nextSlot.pageNum != _footer->pageNum) {
            if (_nextRecord.nextSlot.pageNum == 0)  {
                _eof = true;
                return err::OK;
            }

            ret = _fileHandle.readPage(_nextRecord.nextSlot.pageNum, _buffer);
            RETURN_ON_ERR(ret);
            _nextSlot = _ixfm->getIXSlot(_footer->firstRID.slotNum, _buffer);
            ret  = _ixfm->loadIXRecord(_nextSlot->recordSize, _nextSlot->recordOffset, _buffer, _type, _nextRecord);
            RETURN_ON_ERR(ret);
            continue;
        } else {
            _nextSlot = _ixfm->getIXSlot(_nextRecord.nextSlot.slotNum, _buffer);
            ret  = _ixfm->loadIXRecord(_nextSlot->recordSize, _nextSlot->recordOffset, _buffer, _type, _nextRecord);
            RETURN_ON_ERR(ret);
        }
    }

    while ((not _lowInclusive) and _lowKey.compare(_nextRecord.key) == 0) {
        if (_nextRecord.nextSlot.pageNum != _footer->pageNum) {
            if (_nextRecord.nextSlot.pageNum == 0) {
                _eof = true;
                return err::OK;
            }

            ret = _fileHandle.readPage(_nextRecord.nextSlot.pageNum, _buffer);
            RETURN_ON_ERR(ret);
            _nextSlot = _ixfm->getIXSlot(_footer->firstRID.slotNum, _buffer);
            ret  = _ixfm->loadIXRecord(_nextSlot->recordSize, _nextSlot->recordOffset, _buffer, _type, _nextRecord);
            RETURN_ON_ERR(ret);
            continue;
        } else {
            _nextSlot = _ixfm->getIXSlot(_nextRecord.nextSlot.slotNum, _buffer);
            ret  = _ixfm->loadIXRecord(_nextSlot->recordSize, _nextSlot->recordOffset, _buffer, _type, _nextRecord);
            RETURN_ON_ERR(ret);
        }
    }

    if (_highKey.compare(_nextRecord.key) < 0)
        return _eof;
    
    // Otherwise, all _next* state variables are set properly
    return err::OK;
}

RC IX_ScanIterator::loadNextRecord() 
{
    RC ret;
    if (_nextRecord.nextSlot.pageNum != _footer->pageNum) {
        if (_footer->nextLeaf == 0) 
            return IX_EOF;

        ret = _fileHandle.readPage(_footer->nextLeaf, _buffer);
        RETURN_ON_ERR(ret);
        _nextSlot = _ixfm->getIXSlot(_footer->firstRID.slotNum, _buffer);
        ret = _ixfm->loadIXRecord(_nextSlot->recordSize, _nextSlot->recordOffset, _buffer, _type, _nextRecord);
    } else {
        _nextSlot = _ixfm->getIXSlot(_nextRecord.nextSlot.slotNum, _buffer);
        ret = _ixfm->loadIXRecord(_nextSlot->recordSize, _nextSlot->recordOffset, _buffer, _type, _nextRecord);
    }
    return ret;
}

RC IX_ScanIterator::close()
{
    _ixfm = NULL;
    _footer = NULL;
    _nextSlot = NULL;
    return err::OK;
}

void IX_PrintError (RC rc)
{
    cout << err::errToString(rc) << endl;
}

int KeyData::compare(KeyData &key) {
    switch (type) {
        case TypeInt:
            {
                if (integer < key.integer) return -1;
                else if (integer == key.integer) return 0;
                else return 1;
            }
        case TypeReal:
            {
                if (real < key.real) return -1;
                else if (real == key.real) return 0;
                else return 1;
            }
        case TypeVarChar:
            return strcmp(varchar, key.varchar);
    }
}

string KeyData::toString() {
    switch (type) {
        case TypeInt:
            return to_string(integer);
        case TypeReal:
            return to_string(real);
        case TypeVarChar:
            return string(varchar);
    }
}
