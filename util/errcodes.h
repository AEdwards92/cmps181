#ifndef _errcodes_h_
#define _errcodes_h_

#include <iostream>

#define RETURN_ON_ERR(ret) {if((ret) != err::OK) {cerr << err::errToString(ret) << endl;  return ret;}}

namespace err {
    enum ErrorCode {
        UNKNOWN_FAILURE = -1,
        OK = 0,

        FEATURE_NOT_YET_IMPLEMENTED,

        FILE_PAGE_NOT_FOUND,
        FILE_SEEK_FAILED,
        FILE_NOT_FOUND,
        FILE_ALREADY_EXISTS,
        FILE_CORRUPT,
        FILE_COULD_NOT_OPEN,
        FILE_COULD_NOT_DELETE,
        FILE_NOT_OPENED,

        FILE_HANDLE_ALREADY_INITIALIZED,
        FILE_HANDLE_NOT_INITIALIZED,
        FILE_HANDLE_UNKNOWN,

        HEADER_SIZE_CORRUPT,
        HEADER_PAGESIZE_MISMATCH,
        HEADER_VERSION_MISMATCH,
        HEADER_FREESPACE_LISTS_MISMATCH,
        HEADER_FREESPACE_LISTS_CORRUPT,
        HEADER_SIZE_TOO_LARGE,

        RECORD_DOES_NOT_EXIST,
        RECORD_CORRUPT,
        RECORD_SIZE_INVALID,
        RECORD_EXCEEDS_PAGE_SIZE,
        RECORD_DELETED,

        PAGE_CANNOT_BE_ORGANIZED,

        TABLE_NOT_FOUND,
        TABLE_ALREADY_CREATED,
        TABLE_NAME_TOO_LONG,

        ATTRIBUTE_INVALID_TYPE,
        ATTRIBUTE_NOT_FOUND,
        ATTRIBUTE_NAME_TOO_LONG,
        ATTRIBUTE_COUNT_MISMATCH,

        INDEX_FILE_EXISTS,

        OUT_OF_MEMORY
    };
    const char* errToString(int errnum);
}
#endif
