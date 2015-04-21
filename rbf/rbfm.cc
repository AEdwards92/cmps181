#include "rbfm.h"
#include "../util/errcodes.h"
#include <cstdlib>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
    : _pfm(*(PagedFileManager::instance())) {
}

RecordBasedFileManager::~RecordBasedFileManager() {
    if (_rbf_manager)
        _rbf_manager = NULL;
}


RC RecordBasedFileManager::createFile(const string &fileName) {
    // Request new file from PFM
    RC ret = _pfm.createFile(fileName);
    if (ret != 0) {
        return ret;
    }
    // Open handle to file
    FileHandle fileHandle;
    ret = openFile(fileName, fileHandle);
    if (ret != 0)
        return ret;

    // Create index for page 0
    PageIndex index;
    index.pageNum = 0;
    index.freeMemoryOffset = 0;
    index.numSlots = 0;

    // Write the index at the end of a blank page
    unsigned char buffer[PAGE_SIZE] = {0};
    memcpy(buffer + PAGE_SIZE - sizeof(PageIndex), &index,
           sizeof(PageIndex));
    fileHandle.appendPage(buffer);

    // Drop file handle
    return _pfm.closeFile(fileHandle);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pfm.destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pfm.openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pfm.closeFile(fileHandle);
}

unsigned RecordBasedFileManager::freeSpaceSize(void* pageData) {
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

RC RecordBasedFileManager::findSpace(FileHandle &fileHandle, 
                                     unsigned numbytes,
                                     PageNum& pageNum) {
    RC ret;
    bool pageFound = false;
    unsigned char *buffer[PAGE_SIZE] = {0};
    for(pageNum = 0; pageNum < fileHandle.getNumberOfPages(); pageNum++) {
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
            memset(buffer, 0, PAGE_SIZE);
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
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Figure out how large the stored data is and set pageNum to that
    // determined by findSpace
    PageNum pageNum;
    RC ret = 0;
    const unsigned offsetFieldsSize = sizeof(unsigned) * recordDescriptor.size();
    unsigned recLength = sizeof(unsigned) * recordDescriptor.size();
    unsigned offsetIndex = 0;
    unsigned dataOffset = 0;

    // Allocate an array of offets with N entries, where N is the number of fields as indicated
    // by the recordDescriptor vector. Each entry i in this array points to the address offset,
    // from the base address of the record on disk, where the i-th field is stored. 
    unsigned* offsets = (unsigned*) malloc(offsetFieldsSize);

    for (auto it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
        offsets[offsetIndex++] = recLength;
        int count = 0;
        Attribute attr = *it;
        unsigned attrSize = Attribute::sizeInBytes(attr.type, (char*) data + dataOffset);
        if (attrSize == 0) {
            free(offsets);
            return -1;
        }
        recLength += attrSize;
        dataOffset += attrSize;

        //switch(attr.type) {
        //case TypeInt:
            //recLength += sizeof(int);
            //dataOffset += sizeof(int);
            //break;

        //case TypeReal:
            //recLength += sizeof(float);
            //dataOffset += sizeof(float);
            //break;

        //case TypeVarChar:
            //memcpy(&count, (char*)data + dataOffset, sizeof(int)); 
            //dataOffset += sizeof(int) + count * sizeof(char);
            //recLength += sizeof(int) + count * sizeof(char);
            //break;

        //default:
            //free(offsets);
            //return err::ATTRIBUTE_INVALID_TYPE;
        //}
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
    PageIndex index;
    memcpy((void *) &index, buffer + PAGE_SIZE - sizeof(PageIndex), sizeof(PageIndex));
 
    // Now we write the record at the start of free memeory
    memcpy(buffer + index.freeMemoryOffset, offsets, offsetFieldsSize);
    memcpy(buffer + index.freeMemoryOffset + offsetFieldsSize, data, recLength - offsetFieldsSize);

    free(offsets);

    // Prepare a new index entry to prepend to the list of entries
    PageIndexEntry entry;
    entry.recordSize = recLength;
    entry.recordOffset = index.freeMemoryOffset;
    
    // Copy index entry to list.
    // buffer + PAGE_SIZE - sizeof(PageIndex) - (n + 1) * sizeof(PageIndexEntry)
    // where n = index.numSlots
    memcpy(buffer + PAGE_SIZE - (int)sizeof(PageIndex) - (int)((index.numSlots + 1) * sizeof(PageIndexEntry)), &entry, sizeof(PageIndexEntry));

    // Update index information and write it back to page
    index.numSlots++;
    index.freeMemoryOffset += recLength;
    memcpy(buffer + PAGE_SIZE - sizeof(PageIndex), (void *) &index, sizeof(PageIndex));
 
    // Write page
    ret = fileHandle.writePage(pageNum, buffer);
    if (ret != 0)
        return ret;

    // Once the write is committed, store the RID information and return
    rid.pageNum = pageNum;
    rid.slotNum = index.numSlots - 1;

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Read in the page specified by pageNum
    unsigned char buffer[PAGE_SIZE] = {0};
    RC ret = fileHandle.readPage(rid.pageNum, buffer);
    if (ret != 0) 
        return ret;

    PageIndexEntry entry;
    memcpy(&entry, buffer + PAGE_SIZE - (int)sizeof(PageIndex) - (int)((rid.slotNum + 1) * sizeof(PageIndexEntry)), sizeof(PageIndexEntry));
    int fieldOffset = recordDescriptor.size() * sizeof(unsigned);
    memcpy(data, buffer + entry.recordOffset + fieldOffset, entry.recordSize - fieldOffset);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}

unsigned Attribute::sizeInBytes(AttrType type, const void* value) {
    switch(type)
    {
    case TypeInt:
        return sizeof(int);

    case TypeReal:
        return sizeof(float);

    case TypeVarChar:
        return sizeof(unsigned) + sizeof(char) * ( *(unsigned*)value );

    default:
        return 0;
    }
}
