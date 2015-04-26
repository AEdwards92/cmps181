#include "rbfm.h"
#include "../util/errcodes.h"
#include <cstdlib>
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

RC RecordBasedFileManager::destroyFile(const string &fileName) {
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

void RecordBasedFileManager::writePageIndex(void *buffer, 
                                            PageIndex *index) 
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
    unsigned char *buffer[PAGE_SIZE] = {0};
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

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, 
                                        const vector<Attribute> &recordDescriptor, 
                                        const void *data, 
                                        RID &rid)
{
    // Figure out how large the stored data is and set pageNum to that
    // determined by findSpace
    PageNum pageNum;
    RC ret = 0;
    const unsigned offsetFieldsSize = sizeof(unsigned) * recordDescriptor.size();
    unsigned recLength = sizeof(unsigned) * recordDescriptor.size();
    unsigned offsetIndex = 0;
    unsigned dataOffset = 0;

    unsigned* offsets = (unsigned*) malloc(offsetFieldsSize);

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
    //memcpy(&index, buffer + PAGE_SIZE - sizeof(PageIndex), sizeof(PageIndex));
 
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
    // buffer + PAGE_SIZE - sizeof(PageIndex) - (n + 1) * sizeof(PageIndexEntry)
    // where n = index.numSlots
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
                                      void *data) 
{
    // Read in the page specified by pageNum
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(rid.pageNum, buffer);
    if (ret != 0) 
        return ret;

    PageIndexEntry *entry = getPageIndexEntry(buffer, rid.slotNum);

    // Now that we have the index entry, we must check what
    // type of index entry it is. There are three cases:
    //  - Alive:     Most typical state. Provides the fields
    //               recordSize and recordOffset.
    //  - Dead:      Entry no longer exists. This should trigger
    //               an error.
    //  - Tombstone: The record that used to be here now is elsewhere.
    //               The field tombStoneRID has the new RID. In this 
    //               case we make a recursive call to readRecord.

    switch (entry->type)
    {
        case ALIVE: 
            {
            int fieldOffset = recordDescriptor.size() * sizeof(unsigned);
            memcpy(data, buffer + entry->recordOffset + fieldOffset, entry->recordSize - fieldOffset);
            return err::OK;
            }
        case DEAD:
            return err::RECORD_DOES_NOT_EXIST;
        case TOMBSTONE:
            return readRecord(fileHandle, recordDescriptor, entry->tombStoneRID, data);
    }
}


RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, 
                                        const vector<Attribute> &recordDescriptor, 
                                        const RID &rid)
{
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(rid.pageNum, buffer);
    if (ret != err::OK)
        return ret;

    PageIndex *index = getPageIndex(buffer);
    PageIndexEntry *entry = getPageIndexEntry(buffer, rid.slotNum);

    switch (entry->type)
    {
        case ALIVE: 
            {
                // This record is on this page and should be deleted
                return -1;
            }
        case DEAD:
            return err::RECORD_DOES_NOT_EXIST;
        case TOMBSTONE:
            // Delete this and all subsequent tombstones
            //
            return -1;
    }

    return -1;
}

// Assume the rid does not change after update
RC updateRecord(FileHandle &fileHandle, 
                const vector<Attribute> &recordDescriptor,
                const void *data, 
                const RID &rid)
{
    return -1;
}

RC readAttribute(FileHandle &fileHandle, 
                 const vector<Attribute> &recordDescriptor, 
                 const RID &rid, 
                 const string &attributeName, 
                 void *data)
{
    return -1;
}

// scan returns an iterator to allow the caller to go through the results one by one. 
RC scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator)
{
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
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
