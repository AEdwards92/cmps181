#include "rbfm.h"
#include "../util/errcodes.h"
#include <iostream>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
    : _pfm(*(PagedFileManager::instance())) 
{
}

RecordBasedFileManager::~RecordBasedFileManager() 
{
    _rbf_manager = NULL;
}


RC RecordBasedFileManager::createFile(const string &fileName) 
{
    // Request new file from PFM
    RC ret = _pfm.createFile(fileName);
    if (ret != err::OK) {
        return ret;
    }
    // Open handle to file
    FileHandle fileHandle;
    ret = openFile(fileName, fileHandle);
    if (ret != err::OK)
        return ret;

    // Create index for page 0
    PageIndex index;
    index.pageNum = 0;
    index.freeMemoryOffset = 0;
    index.numSlots = 0;

    // Write the index at the end of a blank page
    unsigned char buffer[PAGE_SIZE] = {0};
    writePageIndex(buffer, &index);
    // Flush buffer
    fileHandle.appendPage(buffer);

    // Drop file handle
    return _pfm.closeFile(fileHandle);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return _pfm.destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, 
                                    FileHandle &fileHandle) 
{
    return _pfm.openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    return _pfm.closeFile(fileHandle);
}

PageIndex* RecordBasedFileManager::getPageIndex(void* buffer)
{
    return (PageIndex*)((char*)buffer + PAGE_SIZE - sizeof(PageIndex));
}

void RecordBasedFileManager::writePageIndex(void* buffer, 
                                            PageIndex* index) 
{
    unsigned offset = PAGE_SIZE - sizeof(PageIndex);
    memcpy((char*)buffer + offset, index, sizeof(PageIndex));
}

PageIndexEntry* RecordBasedFileManager::getPageIndexEntry(void* buffer, 
                                                        unsigned slotNum)
{
    unsigned offset = PAGE_SIZE;
    offset -= sizeof(PageIndex);
    offset -= ((slotNum + 1) * sizeof(PageIndexEntry));
    return (PageIndexEntry*)((char*)buffer + offset);
}

void RecordBasedFileManager::writePageIndexEntry(void* buffer, 
                                                 unsigned slotNum, 
                                                 PageIndexEntry* entry)
{
    unsigned offset = PAGE_SIZE;
    offset -= sizeof(PageIndex);
    offset -= ((slotNum + 1) * sizeof(PageIndexEntry));
    memcpy((char*)buffer + offset, entry, sizeof(PageIndexEntry));
}

unsigned RecordBasedFileManager::freeSpaceSize(void* pageData) 
{
    // Read page index, compute number of bytes between
    // freeMemoryOffset and slots/index data.
    PageIndex index;
    memcpy((void *) &index, (unsigned char *) pageData + PAGE_SIZE - sizeof(PageIndex),
            sizeof(PageIndex));
    unsigned space = PAGE_SIZE - sizeof(PageIndex) 
                               - index.numSlots*sizeof(PageIndexEntry) 
                               - index.freeMemoryOffset;
    return space;
}

// Stores pageNum with the number of the first page that has enought
// space to store numbytes of data
RC RecordBasedFileManager::findSpace(FileHandle &fileHandle, 
                                     unsigned numbytes,
                                     PageNum& pageNum) 
{
    RC ret;
    bool pageFound = false;
    unsigned char buffer[PAGE_SIZE] = {0};
    for (pageNum = 0; pageNum < fileHandle.getNumberOfPages(); pageNum++) {
        ret = fileHandle.readPage(pageNum, buffer);
        if (ret != 0)
            return ret;
        if (freeSpaceSize(buffer) >= numbytes) {
            pageFound = true;
            break;
        }
    }
    if (not pageFound) {
        unsigned requiredPages = 1 + ((numbytes - 1) / PAGE_SIZE);
        for (unsigned i = 0; i < requiredPages; i++) {
            memset(buffer, 0, PAGE_SIZE); // unnecessary?
            PageIndex index;
            index.pageNum = pageNum + i;
            index.freeMemoryOffset = 0;
            index.numSlots = 0;

            // Write the index at the end of a blank page
            memcpy(buffer + PAGE_SIZE - sizeof(PageIndex), &index,
                   sizeof(PageIndex));
            ret = fileHandle.appendPage(buffer);
            if (ret != 0)
                return ret;
        }
    }
    return err::OK;
}

RC RecordBasedFileManager::prepareRecord(const vector<Attribute> &recordDescriptor,
                                         const void* data,
                                         unsigned*& offsets,
                                         unsigned& recLength,
                                         unsigned& offsetFieldsSize)
{
    offsetFieldsSize = sizeof(unsigned) * recordDescriptor.size();
    recLength = offsetFieldsSize;
    unsigned offsetIndex = 0;
    unsigned dataOffset = 0;

    offsets = (unsigned*) malloc(offsetFieldsSize);

    // Determine offsets for each field 
    for (auto it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
        offsets[offsetIndex++] = recLength;
        Attribute attr = *it;
        unsigned attrSize = Attribute::size(attr.type, (char *)data + dataOffset);
        if (attrSize == 0) {
            free(offsets);
            return err::ATTRIBUTE_INVALID_TYPE;
        }
        recLength += attrSize;
        dataOffset += attrSize;
    }
    return err::OK;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, 
                                        const vector<Attribute> &recordDescriptor, 
                                        const void* data, 
                                        RID &rid)
{
    // Figure out how large the stored data is and set pageNum to that
    // determined by findSpace
    PageNum pageNum;
    RC ret = 0;
    unsigned offsetFieldsSize = 0;
    unsigned recLength = 0;
    unsigned* offsets = NULL;

    prepareRecord(recordDescriptor, data, offsets, recLength, offsetFieldsSize);
    ret = findSpace(fileHandle, recLength + sizeof(PageIndexEntry), pageNum);
    if (ret != 0) {
        free(offsets);
        return ret;
    }

    // Read in the page specified by pageNum
    unsigned char buffer[PAGE_SIZE] = {0};
    ret = fileHandle.readPage(pageNum, buffer);
    if (ret != 0) {
        free(offsets);
        return ret;
    }

    // Copy the index
    PageIndex* index = getPageIndex(buffer);
 
    // Now we write the record at the start of free memeory
    memcpy(buffer + index->freeMemoryOffset, offsets, offsetFieldsSize);
    memcpy(buffer + index->freeMemoryOffset + offsetFieldsSize, data, recLength - offsetFieldsSize);

    free(offsets);

    // Prepare a new index entry to prepend to the list of entries
    PageIndexEntry entry;
    entry.type = ALIVE;
    entry.recordSize = recLength;
    entry.recordOffset = index->freeMemoryOffset;
    
    // Copy index entry to list.
    writePageIndexEntry(buffer, index->numSlots, &entry); 

    // Update index information and write it back to page
    index->numSlots++;
    index->freeMemoryOffset += recLength;
 
    // Write page
    ret = fileHandle.writePage(pageNum, buffer);
    if (ret != 0)
        return ret;

    // Once the write is committed, store the RID information and return
    rid.pageNum = pageNum;
    rid.slotNum = index->numSlots - 1;

    return err::OK;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor, 
                                      const RID &rid, 
                                      void* data) 
{
    // Read in the page specified by pageNum
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(rid.pageNum, buffer);
    if (ret != 0) 
        return ret;

    PageIndex* index = getPageIndex(buffer);
    if (rid.slotNum >= index->numSlots) 
        return err::RECORD_DELETED;

    PageIndexEntry* entry = getPageIndexEntry(buffer, rid.slotNum);

    switch (entry->type)
    {
        case ALIVE: 
        case ANCHOR: 
            {
            int fieldOffset = recordDescriptor.size() * sizeof(unsigned);
            memcpy(data, buffer + entry->recordOffset + fieldOffset, entry->recordSize - fieldOffset);
            return err::OK;
            }
        case DEAD:
            return err::RECORD_DELETED;
        case TOMBSTONE:
            return readRecord(fileHandle, recordDescriptor, entry->tombstoneRID, data);
    }
}

RC RecordBasedFileManager::deleteRID(FileHandle& fileHandle,
                                     PageIndex* index,
                                     PageIndexEntry* entry,
                                     unsigned char* buffer,
                                     const RID& rid)
{
    // If this is the last slot, we can recover its storage space
    // as freespace
    //if (rid.slotNum == index->numSlots - 1) {
        //index->freeMemoryOffset -= entry->recordSize;
        //index->numSlots -= 1;
    //} else {
        //entry->recordSize = 0;
        //entry->type = DEAD;
    //}
    entry->recordSize = 0;
    entry->type = DEAD;
    return fileHandle.writePage(rid.pageNum, buffer);
}

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle) 
{
    // Iterate over pages in file and replace each page
    // with an empty page
    unsigned char buffer[PAGE_SIZE] = {0};
    PageIndex* index = getPageIndex(buffer);
    index->freeMemoryOffset = 0;
    index->numSlots = 0;
    unsigned numPages = fileHandle.getNumberOfPages();
    RC ret = 0;
    for (unsigned pageNum = 0; pageNum < numPages; pageNum++) {
        index->pageNum = pageNum;
        ret = fileHandle.writePage(pageNum, buffer);
        if (ret != 0)
            return ret;
    }
    return err::OK;
}


RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, 
                                        const vector<Attribute> &recordDescriptor, 
                                        const RID &rid)
{
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(rid.pageNum, buffer);
    if (ret != err::OK)
        return ret;

    PageIndex* index = getPageIndex(buffer);
    PageIndexEntry* entry = getPageIndexEntry(buffer, rid.slotNum);

    switch (entry->type)
    {
        case ALIVE: 
        case ANCHOR: 
            // This record is on this page and should be deleted
            return deleteRID(fileHandle, index, entry, buffer, rid);
        case DEAD:
            return err::RECORD_DELETED;
        case TOMBSTONE:
            ret = deleteRID(fileHandle, index, entry, buffer, rid);
            if (ret != err::OK) 
                return ret;
            return deleteRecord(fileHandle, recordDescriptor, entry->tombstoneRID);
    }

}

// Assume the rid does not change after update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, 
                const vector<Attribute> &recordDescriptor,
                const void* data, 
                const RID &rid)
{
    RC ret = 0;
    unsigned offsetFieldsSize = 0;
    unsigned recLength = 0;
    unsigned* offsets = NULL;

    prepareRecord(recordDescriptor, data, offsets, recLength, offsetFieldsSize);
    
    // Read in the page specified by the RID
    unsigned char buffer[PAGE_SIZE] = {0};
    ret = fileHandle.readPage(rid.pageNum, buffer);
    if (ret != 0) 
        return ret;

    PageIndex* index = getPageIndex(buffer);
    if (rid.slotNum >= index->numSlots) 
        return err::RECORD_DELETED;

    PageIndexEntry* entry = getPageIndexEntry(buffer, rid.slotNum);

    // First off, check to make sure that the record is not dead
    if (entry->type == DEAD)
        return err::RECORD_DELETED;

    // Next, check if it is a tombstone. If so, make recursive call
    if (entry->type == TOMBSTONE)
        return updateRecord(fileHandle, recordDescriptor, data, entry->tombstoneRID);

    // Otherwise, check if we can update in place
    if (rid.slotNum == index->numSlots - 1) {
        // If this is the last record, and there is enough freespace to keep the updated record
        // in the page, then we do so
        unsigned freespace = freeSpaceSize(buffer);
        if (recLength - entry->recordSize <= freespace) {
            entry->recordSize = recLength;
            index->freeMemoryOffset = entry->recordOffset + entry->recordSize;
            memcpy(buffer + entry->recordOffset, offsets, offsetFieldsSize);
            memcpy(buffer + entry->recordOffset + offsetFieldsSize, data, recLength - offsetFieldsSize);
            return fileHandle.writePage(rid.pageNum, buffer);
        }
    } 

    // In this case, it is not the last record, but if it is small enough we can use the same location
    if (recLength <= entry->recordSize) {
        entry->recordSize = recLength;
        memcpy(buffer + entry->recordOffset, offsets, offsetFieldsSize);
        memcpy(buffer + entry->recordOffset + offsetFieldsSize, data, recLength - offsetFieldsSize);
        return fileHandle.writePage(rid.pageNum, buffer);
    }
    // There is no way to update in place, so we must store updated record in new page and
    // leave a tombstone
    PageNum pageNum;
    unsigned char newBuffer[PAGE_SIZE] = {0};
    ret = findSpace(fileHandle, recLength + sizeof(PageIndexEntry), pageNum);
    if (ret != 0) {
        free(offsets);
        return ret;
    }

    // Read in the page specified by pageNum
    ret = fileHandle.readPage(pageNum, newBuffer);
    if (ret != 0) {
        free(offsets);
        return ret;
    }

    // Copy the index
    PageIndex* newIndex = getPageIndex(newBuffer);
 
    // Now we write the record at the start of free memeory
    memcpy(newBuffer + newIndex->freeMemoryOffset, offsets, offsetFieldsSize);
    memcpy(newBuffer + newIndex->freeMemoryOffset + offsetFieldsSize, data, recLength - offsetFieldsSize);
    free(offsets);

    // Prepare a new index entry to prepend to the list of entries
    PageIndexEntry newEntry;
    newEntry.type = ANCHOR;
    newEntry.recordSize = recLength;
    newEntry.recordOffset = newIndex->freeMemoryOffset;
    
    // Copy index entry to list.
    writePageIndexEntry(newBuffer, newIndex->numSlots, &newEntry); 

    // Update index information and write it back to page
    newIndex->numSlots++;
    newIndex->freeMemoryOffset += recLength;
 
    // Write page
    ret = fileHandle.writePage(pageNum, newBuffer);
    if (ret != 0)
        return ret;

    // After writing, update entry to be a tombstone
    entry->type = TOMBSTONE;
    entry->tombstoneRID.pageNum = pageNum;
    entry->tombstoneRID.slotNum = newIndex->numSlots - 1;

    return fileHandle.writePage(rid.pageNum, buffer);
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, 
                 const vector<Attribute> &recordDescriptor, 
                 const RID &rid, 
                 const string &attributeName, 
                 void* data)
{
    // Pull the page into memory - O(1)
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(rid.pageNum, buffer);
    if (ret != err::OK)
    {
        return ret;
    }

    // Find the attribute index sought after by the caller
    int attrIndex = 0; 
    Attribute attr;
    for (vector<Attribute>::const_iterator itr = recordDescriptor.begin(); itr != recordDescriptor.end(); itr++)
    {
        if (itr->name == attributeName)
        {
            attr = *itr;
            break;
        }
        attrIndex++;
    }

    PageIndexEntry* indexEntry = (PageIndexEntry*)(buffer + PAGE_SIZE - sizeof(PageIndex) - ((rid.slotNum + 1) * sizeof(PageIndexEntry)));

    unsigned char* recBuffer = (unsigned char*)malloc(indexEntry->recordSize);
    memcpy(recBuffer, buffer + indexEntry->recordOffset, indexEntry->recordSize);

    // Determine the offset of the attribute sought after
    unsigned offset = 0;
    memcpy(&offset, recBuffer + (attrIndex * sizeof(unsigned)), sizeof(unsigned));

    // Now read the data into the caller's buffer
    switch (attr.type)
    {
        case TypeInt:
        case TypeReal:
            memcpy(data, recBuffer + offset, sizeof(unsigned));
            break;
        case TypeVarChar:
            int dataLen = 0;
            memcpy(&dataLen, recBuffer + offset, sizeof(unsigned));
            memcpy(data, &dataLen, sizeof(unsigned));
            memcpy((char*)data + sizeof(unsigned), recBuffer + offset + sizeof(unsigned), dataLen);
            break;
    }

    // Free up the memory
    free(recBuffer);
    return err::OK;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, 
                                          const vector<Attribute> &recordDescriptor, 
                                          const unsigned pageNumber)
{
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(pageNumber, buffer);
    if (ret != err::OK)
        return ret;

    if (pageNumber == 3) {
        ret = 0;
    }

    // Get index to determine number of slots
    PageIndex* index = getPageIndex(buffer);
    PageIndex newIndex;
    newIndex.numSlots = index->numSlots;
    newIndex.pageNum = index->pageNum;
    newIndex.freeMemoryOffset = index->freeMemoryOffset;

    if (index->numSlots == 0)
        return err::OK; // Should this be an error?

    // indexEntryList[i] = i^th from the last PageIndexEntry (0^th from last is last)
    PageIndexEntry* indexEntryList = new PageIndexEntry[index->numSlots];
    unsigned offset = PAGE_SIZE - sizeof(PageIndex) - (index->numSlots * sizeof(PageIndexEntry));
    memcpy(indexEntryList, buffer + offset, index->numSlots * sizeof(PageIndexEntry));
 
    // Find first ALIVE index entry
    int entryIndex;
    for (entryIndex = index->numSlots - 1; entryIndex >= 0; entryIndex--) {
        if (indexEntryList[entryIndex].type == ALIVE || indexEntryList[entryIndex].type == ANCHOR)
            break;
    }

    if (entryIndex == -1) {
        // All index entries are tombstones or dead.
        // Therefore, we can reset the freeMemoryOffset to 0, all relevant info
        // on the page is contained within the slot entry.
        index->freeMemoryOffset = 0;
        return fileHandle.writePage(pageNumber, buffer);
    }

    unsigned char newBuffer[PAGE_SIZE] = {0};
    offset = 0;
    for ( ; ; ) {
        // Copy old record, update its recordOffset
        memcpy(newBuffer + offset, buffer + indexEntryList[entryIndex].recordOffset, indexEntryList[entryIndex].recordSize);
        indexEntryList[entryIndex].recordOffset = offset;
        offset += indexEntryList[entryIndex].recordSize;
        entryIndex -= 1;
        while (entryIndex >= 0) {
            if (indexEntryList[entryIndex].type == ALIVE || indexEntryList[entryIndex].type == ANCHOR)
                break;
            entryIndex -= 1;
        }
        if (entryIndex == -1)
            break;
    }
    // Update freeMemoryOffset for new page
    newIndex.freeMemoryOffset = offset;
    writePageIndex(newBuffer, &newIndex);
    // Copy the updated PageIndexEntry's to new page
    offset = PAGE_SIZE - sizeof(PageIndex) - (newIndex.numSlots * sizeof(PageIndexEntry));
    memcpy(newBuffer + offset, indexEntryList, newIndex.numSlots * sizeof(PageIndexEntry));
    // Finally, write the compacted page to disk
    return fileHandle.writePage(pageNumber, newBuffer);
}

// scan returns an iterator to allow the caller to go through the results one by one. 
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,                  // comparision type such as "<" and "="
                                const void* value,                    // used in the comparison
                                const vector<string> &attributeNames, // a list of projected attributes
                                RBFM_ScanIterator &rbfm_ScanIterator)
{
    return rbfm_ScanIterator.init(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}

RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, 
                                          const vector<Attribute> &recordDescriptor) 
{
    return -1;

}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, 
                                       const void* data)
{
    unsigned index = 0;
    int offset = 0;
    std::ostream& out = std::cout;
    out << "(";
    for (vector<Attribute>::const_iterator itr = recordDescriptor.begin(); itr != recordDescriptor.end(); itr++)
    {
        Attribute attr = *itr;
        switch (attr.type)
        {
            case TypeInt:
            {
                int ival = 0;
                memcpy(&ival, (char*)data + offset, sizeof(int));
                offset += sizeof(int);
                out << ival;
                break;
            }
            case TypeReal:
            {
                float rval = 0.0;
                memcpy(&rval, (char*)data + offset, sizeof(float));
                offset += sizeof(float);
                out << rval;
                break;
            }
            case TypeVarChar:
            {
                out << "\"";
                int count = 0;
                memcpy(&count, (char*)data + offset, sizeof(int));
                offset += sizeof(int);
                for (int i=0; i < count; i++)
                {
                    out << ((char*)data)[offset++];
                }
                out << "\"";
            }
        }
        index++;
        if (index != recordDescriptor.size()) out << ",";
    }

    out << ")" << endl;


    return 0;
}

unsigned Attribute::size(AttrType type, const void* value)
{
    switch(type) {
        case TypeInt:
            return sizeof(int);
        case TypeReal:
            return sizeof(float);
        case TypeVarChar:
            return sizeof(unsigned) + sizeof(char) * (* (unsigned *)value);
        default:
            return 0;
    }
}

RC RBFM_ScanIterator::close()
{
     free(_compValue);
     _compValue = NULL;
     _fileHandle = NULL;
     _compIndex = -1;
     _returnAttrIndices.clear();
     _returnAttrTypes.clear();
   
     return err::OK;
}

RC RBFM_ScanIterator::init(FileHandle& fileHandle, 
                           const vector<Attribute> &recordDescriptor,
                           const string &conditionAttribute,
                           const CompOp compOp,
                           const void* value,
                           const vector<string> &attributeNames)
{
	RC ret = err::OK;
    _fileHandle = &fileHandle;
    _recordDescriptor = recordDescriptor;
	_compOp = compOp;
	_nextRID.pageNum = 0;
	_nextRID.slotNum = 0;
	
	if (compOp != NO_OP) {
		ret = lookupAttr(conditionAttribute, _compIndex);
		if (ret != err::OK) 
			return ret;
		_compType = _recordDescriptor[_compIndex].type;
		copyCompValue(_compType, value);
	}

	_returnAttrTypes.clear();
	_returnAttrIndices.clear();
	for (auto it = attributeNames.begin(); it != attributeNames.end(); ++it) {
		unsigned index;
		ret = lookupAttr(*it, index);
		if (ret != err::OK)
			return ret;

		_returnAttrTypes.push_back(_recordDescriptor[index].type);
		_returnAttrIndices.push_back(index);
	}

	return err::OK;
}

RC RBFM_ScanIterator::getNextRecord(RID& rid, 
                                    void* data)
{
    unsigned numPages = _fileHandle->getNumberOfPages();
    if (_nextRID.pageNum >= numPages)
        return RBFM_EOF;

    char buffer[PAGE_SIZE] = {0};
    RC ret = _fileHandle->readPage(_nextRID.pageNum, buffer);
    if (ret != err::OK)
        return ret;

    PageIndex* index = RecordBasedFileManager::getPageIndex(buffer);

    unsigned currentNumSlots = index->numSlots;
    PageNum currentPage = _nextRID.pageNum;
    while (_nextRID.pageNum < numPages) {
        if (_nextRID.pageNum != currentPage) {
            currentPage = _nextRID.pageNum;
            RC ret = _fileHandle->readPage(_nextRID.pageNum, buffer);
            if (ret != err::OK)
                return ret;
            currentNumSlots = index->numSlots;
        }
        if (_nextRID.slotNum >= currentNumSlots) {
            updateNextRecord(currentNumSlots);
            continue;
        }
        // To avoid duplicate return values, and so RID's stay consistent, only check ALIVE
        // and TOMBSTONE records. If it is a TOMBSTONE then we must load buffer with the page
        // containing the actual record data
        PageIndexEntry* entry = RecordBasedFileManager::getPageIndexEntry(buffer, _nextRID.slotNum);
        switch (entry->type) {
            case DEAD:
            case ANCHOR:
                updateNextRecord(currentNumSlots);
                continue;
            case TOMBSTONE: {
                RID newRID;
                while (entry->type == TOMBSTONE) {
                    newRID = entry->tombstoneRID;
                    ret = _fileHandle->readPage(newRID.pageNum, buffer);
                    currentPage = newRID.pageNum;
                    if (ret != err::OK)
                        return ret;
                    entry = RecordBasedFileManager::getPageIndexEntry(buffer, newRID.slotNum);
                }
            }
            default: 
                break;
        }
        // Now entry points to an entry whose physical data is on buffer
        // We are ready to test the scan condition
        if (not testScan(buffer + entry->recordOffset)) {
            updateNextRecord(currentNumSlots);
            continue;
        }
        // If we are here, then we passed. Copy the desired attributes to the user buffer
        rid = _nextRID;
        copyRecord((char*) data, buffer + entry->recordOffset);
        updateNextRecord(currentNumSlots);
        return err::OK;
    }
    return RBFM_EOF;
}

RC RBFM_ScanIterator::lookupAttr(const string& conditionAttribute,
                                 unsigned& index)
{
    index = 0;
    for (auto it = _recordDescriptor.begin(); it != _recordDescriptor.end(); it++)
    {
        if (it->name == conditionAttribute) 
            return err::OK;

        index++;
    }
    return err::ATTRIBUTE_NOT_FOUND;
}

RC RBFM_ScanIterator::copyCompValue(AttrType attrType, 
                                    const void* value)
{
    free(_compValue);
    unsigned attrSize = Attribute::size(attrType, value);
    _compValue = malloc(attrSize);
    if (not _compValue)
        return err::OUT_OF_MEMORY;
    memcpy(_compValue, value, attrSize);
    return err::OK;
}

bool RBFM_ScanIterator::testScan(const void* recData)
{
    if (_compOp == NO_OP)
        return true;

    unsigned* offsets = (unsigned*)recData;
    float floatVal;
    int intVal;

    switch (_compType)
    {
        case TypeInt:
            intVal = *(int*) ((char *)recData + offsets[_compIndex]);
            return doComp(_compOp, &intVal, (int*) _compValue);
        case TypeReal:
            floatVal = *(float*) ((char *)recData + offsets[_compIndex]);
            return doComp(_compOp, &floatVal, (float*) _compValue);
        case TypeVarChar:
            void* attrData = (unsigned*) recData + offsets[_compIndex] + sizeof(unsigned);
            void* compStr = (unsigned*) _compValue + sizeof(unsigned);
            return doComp(_compOp, (char*) attrData, (char*) compStr);
    }
    return false;
}

bool RBFM_ScanIterator::doComp(const CompOp compOp, const int* attrData, const int* value)
{
    switch (compOp) {
        case NO_OP: return true;
        case EQ_OP: return *attrData == *value;
        case LT_OP: return *attrData <  *value; 
        case GT_OP: return *attrData >  *value; 
        case LE_OP: return *attrData <= *value; 
        case GE_OP: return *attrData >= *value; 
        case NE_OP: return *attrData != *value; 
    }
    return false;
}

bool RBFM_ScanIterator::doComp(const CompOp compOp, const float* attrData, const float* value)
{
    switch (compOp) {
        case NO_OP: return true;
        case EQ_OP: return *attrData == *value;
        case LT_OP: return *attrData <  *value; 
        case GT_OP: return *attrData >  *value; 
        case LE_OP: return *attrData <= *value; 
        case GE_OP: return *attrData >= *value; 
        case NE_OP: return *attrData != *value; 
    }
    return false;
}

bool RBFM_ScanIterator::doComp(const CompOp compOp, const char* attrData, const char* value)
{
    int strComp = strcmp(attrData, value);
    switch (compOp) {
        case NO_OP: return true;
        case EQ_OP: return strComp == 0;
        case LT_OP: return strComp <  0; 
        case GT_OP: return strComp >  0; 
        case LE_OP: return strComp <= 0; 
        case GE_OP: return strComp >= 0; 
        case NE_OP: return strComp != 0; 
    }
    return false;
}

RC RBFM_ScanIterator::updateNextRecord(unsigned numSlots)
{
	_nextRID.slotNum++;
	if (_nextRID.slotNum >= numSlots) {
		_nextRID.pageNum++;
		_nextRID.slotNum = 0;
	}
    return err::OK;
}

RC RBFM_ScanIterator::copyRecord(char* dest, 
                                 const char* src)
{
	// The offset array is just after the number of attributes
	unsigned* offsets = (unsigned*)((char*)src);
	unsigned dataOffset = 0;

	// Iterate through all of the columns we actually want to copy for the user
	for (unsigned i = 0; i < _returnAttrIndices.size(); ++i) {
		unsigned attrIndex = _returnAttrIndices[i];
		unsigned recordOffset = offsets[attrIndex];
		unsigned attributeSize = Attribute::size(_returnAttrTypes[i], src + recordOffset);

		// Copy the data and then move forward in the user's buffer
		memcpy(dest + dataOffset, src + recordOffset, attributeSize);
		dataOffset += attributeSize;
	}
    return err::OK;
}


