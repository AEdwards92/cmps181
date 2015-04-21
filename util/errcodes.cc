#include "errcodes.h"

using namespace rc;

string errToString(int errnum) {
    switch ((ErrorCode) errnum) {
        case FILE_NOT_FOUND:         return "FILE_NOT_FOUND";
        case FILE_ALREADY_EXISTS:    return "FILE_ALREADY_EXISTS";
        case FILE_PAGE_NOT_FOUND:    return "FILE_PAGE_NOT_FOUND";
        case FILE_SEEK_FAILED:       return "FILE_SEEK_FAILED";
        case FILE_CORRUPT:           return "FILE_CORRUPT";     
        case FILE_OPEN_FAILURE:      return "FILE_OPEN_FAILURE";
        case FILE_REMOVE_FAILURE:    return "FILE_REMOVE_FAILURE";
        case FILE_NOT_OPENED:        return "FILE_NOT_OPENED";
        case FILE_WRITE_ERROR:       return "FILE_WRITE_ERROR";
        case FILE_HEADER_INVALID:    return "FILE_HEADER_INVALID";
        case FILE_HANDLE_ACTIVE:     return "FILE_HANDLE_ACTIVE";
        case FILE_HANDLE_INACTIVE:   return "FILE_HANDLE_INACTIVE";
        case ATTRIBUTE_INVALID_TYPE: return "ATTRIBUTE_INVALID_TYPE";
    }
    return "UNKNOWN_ERROR_CODE";
}
    
