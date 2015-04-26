#include "pfm.h"
#include "../util/errcodes.h"

#include <cstdio>
#include <sys/stat.h>

//////////////////////////////////
// PagedFileManager Implementation
//////////////////////////////////

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance() {
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager() {
}


PagedFileManager::~PagedFileManager() {
    if (_pf_manager)
        _pf_manager = NULL;
}

// Creates an empty paged-file called fileName. The file must not already
// already exist. This method does not create any pages in the file.

RC PagedFileManager::createFile(const string &fileName) {
    if (fileExists(fileName))
        return err::FILE_ALREADY_EXISTS;
    FILE *file;
    file = fopen(fileName.c_str(), "wb");
    if (file == NULL)
        return err::FILE_COULD_NOT_OPEN; 

    if (fputs(SIGNATURE, file) == EOF)
        return err::FILE_CORRUPT; 

    fclose(file);

    return 0;
}

// Destroys the paged file fileName. The file must already exist and
// must have been created with createFile.

RC PagedFileManager::destroyFile(const string &fileName) {
    FILE *file;
    char signature[SIGNATURE_SIZE + 1];
    if (not fileExists(fileName))
        return err::FILE_COULD_NOT_DELETE;

    file = fopen(fileName.c_str(), "rb");
    if (file == NULL)
        return err::FILE_COULD_NOT_OPEN; 
    fgets(signature, SIGNATURE_SIZE + 1, file);

    if (strcmp(signature, SIGNATURE) != 0)
        return err::FILE_CORRUPT;

    if (fclose(file) == EOF)
        return err::FILE_CORRUPT; // Error closing file

    if (handleCount.find(fileName) != handleCount.end()
        and handleCount[fileName] > 0)
        return err::FILE_COULD_NOT_DELETE;
    
    if (remove(fileName.c_str()) != 0)
        return err::FILE_COULD_NOT_DELETE;

    return 0;
}

// Opens the paged file fileName. The file must already exist, and it must
// have been created by a call to createFile. 
//
// If successful, fileHandle now provides access the content of fileName.
// The object fileHandle must not currently be a handle for some open file.
//
// It is not an error to open the same file more than once. Each call to
// openFile creates a new instance of the open file. Opening a file more
// than once for writing is not prevented, but behavior is undefined.
// Opening a file more than once for reading is no problem.

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    // fileHandle must not be handle for some open file
    if (fileHandle.hasFile())
        return err::FILE_HANDLE_ALREADY_INITIALIZED;

    if (not fileExists(fileName))
        return err::FILE_NOT_FOUND;

    FILE *file;
    char signature[SIGNATURE_SIZE + 1];
    file = fopen(fileName.c_str(), "rb+");
    if (file == NULL)
        return err::FILE_COULD_NOT_OPEN; 
    fgets(signature, SIGNATURE_SIZE + 1, file);

    // must be created by PFM
    if (strcmp(signature, SIGNATURE) != 0)
        return err::FILE_CORRUPT;

    if (handleCount.find(fileName) != handleCount.end())
        handleCount[fileName] += 1;
    else
        handleCount[fileName] = 1;

    fileHandle.fileName = fileName;
    return fileHandle.loadFile(file);
}

// Closes the open file referred to by fileHandle. The file should have been
// opened by a call to openFile. All of the file's pages are written to disk
// when the file is closed.

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if (not fileHandle.hasFile())
        return err::FILE_HANDLE_NOT_INITIALIZED;

    handleCount[fileHandle.fileName] -= 1;

    fileHandle.unloadFile();

    return 0;
}

// Checks if a file already exists.
bool PagedFileManager::fileExists(const string &fileName) {
    struct stat buffer;

    if(stat(fileName.c_str(), &buffer) == 0) 
        return true;
    else 
        return false;
}

////////////////////////////
// FileHandle Implementation
////////////////////////////

FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
    _pageCounter = 0;
}


FileHandle::~FileHandle() {
    unloadFile();
}


// Reads the page into the memory block pointed to by data. The page must
// exist. 

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pageNum >= _pageCounter) // note: pages are zero-indexed
        return err::FILE_PAGE_NOT_FOUND;

    if (fseek(_file, SIGNATURE_SIZE + PAGE_SIZE * pageNum, SEEK_SET) != 0)
        return err::FILE_SEEK_FAILED;

    if (fread(data, PAGE_SIZE, 1, _file) != 1)
        return err::FILE_CORRUPT;

    readPageCounter++;
    return 0;
}

// Writes the given data into the page specified by pageNum. The page must
// exist.

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum >= _pageCounter) // note: pages are zero-indexed
        return err::FILE_PAGE_NOT_FOUND;

    if (fseek(_file, SIGNATURE_SIZE + PAGE_SIZE * pageNum, SEEK_SET) != 0)
        return err::FILE_SEEK_FAILED;

    if (fwrite(data, PAGE_SIZE, 1, _file) != 1)
        return err::FILE_CORRUPT;

    writePageCounter++;
    return 0;
}

// This method appends a new page to the end of the file and writes the
// given data into the newly allocated page.

RC FileHandle::appendPage(const void *data)
{
    if (fseek(_file, SIGNATURE_SIZE + _pageCounter * PAGE_SIZE, SEEK_SET) != 0)
        return err::FILE_SEEK_FAILED;

    if (fwrite(data, PAGE_SIZE, 1, _file) != 1)
        return err::FILE_CORRUPT;

    _pageCounter++;
    appendPageCounter++;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    return _pageCounter;
}

// Loads the current counter variables into the three given parameters.

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

RC FileHandle::loadFile(FILE *file) {
    _file = file;
    updatePageCounter();
    return 0;
}

RC FileHandle::unloadFile() {
    if (_file)
        fclose(_file);

    _file = NULL;
    return 0;
}

RC FileHandle::updatePageCounter() {
    if (not hasFile())
        return err::FILE_HANDLE_NOT_INITIALIZED;

    if (fseek(_file, 0, SEEK_END) != 0)
        return err::FILE_SEEK_FAILED;

    _pageCounter = ftell(_file) / PAGE_SIZE;
    return 0;
}
