#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "pfm.h"

using namespace std;

// Record ID
struct RID {
    unsigned pageNum;	// page number
    unsigned slotNum; // slot number in the page
};

// Page Index
struct PageIndex {
    unsigned pageNum;
    unsigned freeMemoryOffset;
    unsigned numSlots;
};

enum PageIndexEntryType { ALIVE = 1, DEAD, TOMBSTONE };

// Page Index Entry
// Contains information for accessing record in page
struct PageIndexEntry {
    enum PageIndexEntryType type;
    union {
        struct {
            unsigned recordSize;
            unsigned recordOffset;
        };
        RID tombStoneRID;
    };
};

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length

    static unsigned size(AttrType type, const void* value);
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { NO_OP = 0,  // no condition
		   EQ_OP,      // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
} CompOp;


/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

  // "data" follows the same format as RecordBasedFileManager::insertRecord()
  RC getNextRecord(RID &rid, void* data) { return RBFM_EOF; };
  RC close() { return -1; };
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle); 

  PageIndex* getPageIndex(void* buffer);

  void writePageIndex(void* buffer, PageIndex* index);

  PageIndexEntry* getPageIndexEntry(void* buffer, unsigned slotNum);

  void writePageIndexEntry(void* buffer, unsigned slotNum, PageIndexEntry* entry);

  unsigned freeSpaceSize(void* pageData);

  // Find somewhere to insert numbytes bytes of data.
  // If there is no room, append enough pages at the end of the file
  // to fit the data.
  RC findSpace(FileHandle &fileHandle, unsigned numbytes, PageNum& pageNum);
  RC prepareRecord(const vector<Attribute> &recordDescriptor, const void* data, unsigned*& offsets, unsigned& recLength, unsigned& offsetFieldsSize);
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void* data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void* data);
  
  // This method will be mainly used for debugging/testing
  RC printRecord(const vector<Attribute> &recordDescriptor, const void* data);

/**************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
***************************************************************************************************************************************************************/
  RC deleteRID(FileHandle& fileHandle, PageIndex* index, PageIndexEntry* entry,
               unsigned char* buffer, const RID& rid);
  
  RC deleteRecords(FileHandle &fileHandle);

  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the rid does not change after update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void* data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void* data);

  RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void* value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

  RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager* _rbf_manager;
  PagedFileManager& _pfm;
};

#endif
