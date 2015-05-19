#ifndef _pfm_h_
#define _pfm_h_

typedef int RC;
typedef char byte;
typedef unsigned PageNum;

#define PAGE_SIZE 4096
#define SIGNATURE "PAGEFILE"
#define SIGNATURE_SIZE 8
        
#include <string>
#include <map>
#include <climits>
using namespace std;

class FileHandle;


// The PagedFileManager (PFM) class handles the creation, deletion, opening, 
// and closing of paged files. The PFM provides facilities for higher-level
// client components to perform file I/O in terms of pages.

class PagedFileManager {
  public:
    // Access to the _pf_manager instance
    static PagedFileManager* instance();

    // Public interface
    RC createFile(const string &fileName);
    RC destroyFile(const string &fileName);
    RC openFile(const string &fileName, FileHandle &fileHandle);
    RC closeFile(FileHandle &fileHandle);

  protected:
    PagedFileManager();
    ~PagedFileManager();

  private:
    static PagedFileManager *_pf_manager;
    map<string,int> handleCount;
    bool fileExists(const string &fileName);
};


// The FileHandle class provides access to the pages of an open file. To
// access the pages of a file, a client first creates an instance of this
// class and passes it to the PagedFileManager::openFile method.
//
// Each FileHandle instance keeps track of the number of reads, writes, and
// appended pages.

class FileHandle {
  public:
    // let PFM be friend
    friend PagedFileManager;
    // variables to keep counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
	
    FileHandle();
    ~FileHandle();

    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC appendPage(const void *data);
    unsigned getNumberOfPages();
    RC collectCounterValues(unsigned &readPageCount, 
                            unsigned &writePageCount, 
                            unsigned &appendPageCount);

    bool hasFile() const { return _file != NULL; }
    FILE* getFile() { return _file; }
    RC loadFile(FILE* file);
    RC unloadFile();
    RC updatePageCounter();
    string getFileName() { return fileName; }
    bool operator== (const FileHandle& that) const { 
                        return this->_file == that._file; }

  private:
    FILE *_file = NULL;
    string fileName;
    unsigned _pageCounter;
}; 

#endif
