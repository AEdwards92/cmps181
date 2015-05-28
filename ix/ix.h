#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define MAX_VARCHAR_SIZE 2048

class IX_ScanIterator;

struct KeyData {
    AttrType type;
    unsigned size; // size in bytes of the physical data, NOT of this struct
    union {
        int integer;
        float real;
        char varchar[sizeof(unsigned) + MAX_VARCHAR_SIZE];
    };
    int compare(KeyData&);// TODO
    string toString();
};

struct IndexRecord {
    KeyData key;
    RID rid;
    RID nextSlot;
    IndexRecord() = default;
    IndexRecord(const IndexRecord& that);
};

struct IndexFileHeader {
    PageNum root; // location of root page
    AttrType type;
    // Leave possibility for more bookkeeping later
};

struct IndexPageFooter {
    unsigned pageNum;
    unsigned freeMemoryOffset;
    unsigned numSlots;
    RID firstRID;
    bool isLeaf;
    union {
        PageNum child;
        PageNum nextLeaf;
    };
};


struct IndexSlot { 
    enum PageIndexEntryType type;
    unsigned recordSize;
    unsigned recordOffset;
};

// Functions that will need to be implemented:
//
//  getFooter(pageBuffer)
//      Return pointer to footer of the pageBuffer
//
//  loadLeafPage(fileHandle, KeyData, pageNum)
//      Update pageNum to the page number of the leaf page
//      where the key belongs. This may need some helper 
//      functions.
//
//  needsSplit(fileHanlde, IndexRecord, pageNum)
//      Returns true if page needs to split: i.e. page cannot
//      hold the IndexRecord
//
//  splitPage(fileHanlde, pageBuffer, pageNum)
//      Allocate two new buffers. In order, start copying 
//      data into one of the buffers until it is half full.
//      Then, add the remaining entries into the old one.
//      Link up the new pages in their IndexPageFooter
//
//  insertIndexRecord(fileHandle, pageBuffer, RID, IndexRecord) 
//      Inserts the IndexRecord to the passed in pageBuffer
//      Simply writes the data to the pageBuffer and has the
//      fileHandle write pageBuffer to the actual file. 
//
//  readRootPage(fileHandle, pageBuffer)
//      Load pageBuffer with the root page of the B-Tree in
//      fileHandle
//
//

class IndexManager {
    public:
        static IndexManager* instance();

        RC newPage(FileHandle &fileHandle,
                   const bool isLeaf,
                   const PageNum num);

        RC initPage(FileHandle &fileHandle,
                   const PageNum pageNum, 
                   const bool isLeaf,
                   const PageNum num,
                   unsigned char* buffer);

        RC newRootPage(FileHandle &fileHandle,
                       const PageNum num,
                       AttrType type,
                       IndexRecord& divider);

        RC createFile(const string &fileName);

        RC destroyFile(const string &fileName);

        RC openFile(const string &fileName, FileHandle &fileHandle);

        RC closeFile(FileHandle &fileHandle);

        // Get footer
        IndexPageFooter* getIXFooter(const void* buffer);

        // Write slot
        void writeIXSlot(void* buffer, unsigned slotNum, IndexSlot* slot);

        // Get slot
        IndexSlot* getIXSlot(const int slotNum, const void* buffer);

        // Load IX record into a struct
        RC loadIXRecord(unsigned size, unsigned offset, unsigned char* buffer, AttrType type, IndexRecord &record);

        PageNum rootPageNum(FileHandle &fileHandle);

        // Load buffer with the root page
        // Assumes fileHandle is active
        RC loadRootPage(FileHandle &fileHandle, void* buffer);

        RC loadKeyData(const void* data, Attribute attr, KeyData& key);

        RC loadHighestKey(FileHandle& fileHandle, AttrType type, KeyData& key);
        // Find the appropriate leafPage for a key to be inserted
        RC loadLeafPage(FileHandle &fileHandle, KeyData &key, AttrType type, vector<PageNum>& parents, unsigned char* buffer);

        bool needsToSplit (KeyData& key, IndexPageFooter* footer);

        RC splitHandler(FileHandle& fileHandle, vector<PageNum>& parents, const AttrType type, KeyData key, RID rid, unsigned char* buffer);
        RC writeRecordToBuffer(KeyData& key, const RID& rid, const RID& nextSlot, AttrType type, IndexPageFooter* footer, unsigned char* buffer);

        RC insertInOrder(KeyData& key, AttrType type, const RID &rid, unsigned char* buffer);

        // The following two functions are using the following format for the passed key value.
        //  1) data is a concatenation of values of the attributes
        //  2) For int and real: use 4 bytes to store the value;
        //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
        RC insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Insert new index entry
        RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Delete index entry

        // scan() returns an iterator to allow the caller to go through the results
        // one by one in the range(lowKey, highKey).
        // For the format of "lowKey" and "highKey", please see insertEntry()
        // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
        // should be included in the scan
        // If lowKey is null, then the range is -infinity to highKey
        // If highKey is null, then the range is lowKey to +infinity
        RC scan(FileHandle &fileHandle,
                const Attribute &attribute,
                const void        *lowKey,
                const void        *highKey,
                bool        lowKeyInclusive,
                bool        highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

    protected:
        IndexManager   ();                            // Constructor
        ~IndexManager  ();                            // Destructor

    private:
        static IndexManager *_index_manager;
        PagedFileManager& _pfm;
        map<string, PageNum> _rootMap;
};

class IX_ScanIterator {
    public:
        IX_ScanIterator();  							// Constructor
        ~IX_ScanIterator(); 							// Destructor

        RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
        RC init(FileHandle &fileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive);
        RC close();             						// Terminate index scan
    private:
        IndexManager* _ixfm;
        FileHandle* _fileHandle;
        AttrType _type;
        KeyData _lowKey;
        KeyData _highKey;
        bool _lowInclusive;
        bool _highInclusive;
        unsigned char _buffer[PAGE_SIZE] = {0};
        IndexPageFooter* _footer;

        bool _eof;

        IndexSlot* _nextSlot;
        IndexRecord _nextRecord;
        IndexRecord _highestRecord;

        RC loadFirstRecord(); 
        RC loadNextRecord();
        RC loadLowestRecord();
        RC loadHighestRecord();
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
