#ifndef _errcodes_h_
#define _errcodes_h_

namespace err {
    enum ErrorCode {
        FILE_NOT_FOUND = 1,
        FILE_ALREADY_EXISTS,
        FILE_PAGE_NOT_FOUND,
        FILE_SEEK_FAILED,
        FILE_CORRUPT,
        FILE_OPEN_FAILURE,
        FILE_REMOVE_FAILURE,
        FILE_NOT_OPENED,
        FILE_WRITE_ERROR,
        FILE_HEADER_INVALID,

        FILE_HANDLE_ACTIVE,
        FILE_HANDLE_INACTIVE,
        ATTRIBUTE_INVALID_TYPE
    };
    string errToString(int errnum);
}
#endif
