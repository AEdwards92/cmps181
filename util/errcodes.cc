#include "errcodes.h"

namespace err
{
    const char* errToString(int errnum) {
        switch ((ErrorCode) errnum) 
        {
            case UNKNOWN_FAILURE:                       return "UNKNOWN_FAILURE";
            case OK:                                    return "OK";
            case FEATURE_NOT_YET_IMPLEMENTED:           return "FEATURE_NOT_YET_IMPLEMENTED";
            case FILE_PAGE_NOT_FOUND:                   return "FILE_PAGE_NOT_FOUND";
            case FILE_SEEK_FAILED:                      return "FILE_SEEK_FAILED";
            case FILE_NOT_FOUND:                        return "FILE_NOT_FOUND";
            case FILE_ALREADY_EXISTS:                   return "FILE_ALREADY_EXISTS";
            case FILE_CORRUPT:                          return "FILE_CORRUPT";
            case FILE_COULD_NOT_OPEN:                   return "FILE_COULD_NOT_APPEND";
            case FILE_COULD_NOT_DELETE:                 return "FILE_COULD_NOT_DELETE";
            case FILE_HANDLE_ALREADY_INITIALIZED:       return "FILE_HANDLE_ALREADY_INITIALIZED";
            case FILE_HANDLE_NOT_INITIALIZED:           return "FILE_HANDLE_NOT_INITIALIZED";
            case FILE_HANDLE_UNKNOWN:                   return "FILE_HANDLE_UNKNOWN";
            case FILE_NOT_OPENED:                       return "FILE_NOT_OPENED";
            case HEADER_SIZE_CORRUPT:                   return "HEADER_SIZE_CORRUPT";
            case HEADER_PAGESIZE_MISMATCH:              return "HEADER_PAGESIZE_MISMATCH";
            case HEADER_VERSION_MISMATCH:               return "HEADER_VERSION_MISMATCH";
            case HEADER_FREESPACE_LISTS_MISMATCH:       return "HEADER_FREESPACE_LISTS_MISMATCH";
            case HEADER_FREESPACE_LISTS_CORRUPT:        return "HEADER_FREESPACE_LIST_CORRUPT";
            case HEADER_SIZE_TOO_LARGE:                 return "HEADER_SIZE_TOO_LARGE";
            case RECORD_DOES_NOT_EXIST:                 return "RECORD_DOES_NOT_EXIST";
            case RECORD_CORRUPT:                        return "RECORD_CORRUPT";
            case RECORD_EXCEEDS_PAGE_SIZE:              return "RECORD_EXCEEDS_PAGE_SIZE";
            case RECORD_SIZE_INVALID:                   return "RECORD_SIZE_INVALID";
            case RECORD_DELETED:                        return "RECORD_IS_DELETED";
            case PAGE_CANNOT_BE_ORGANIZED:              return "PAGE_CANNOT_BE_ORGANIZED";
            case TABLE_NOT_FOUND:                       return "TABLE_NOT_FOUND";
            case TABLE_ALREADY_CREATED;                 return "TABLE_ALREADY_CREATED";
            case TABLE_NAME_TOO_LONG:                   return "TABLE_NAME_TOO_LONG";
            case ATTRIBUTE_INVALID_TYPE:                return "ATTRIBUTE_INVALID_TYPE";
            case OUT_OF_MEMORY:                         return "OUT_OF_MEMORY";
        }

        return "UNKNOWN_ERROR_CODE";
    }
}

