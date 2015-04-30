
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    // Check if catalog file exists, if not create one
    // Open catalog file
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    // Lookup tableName in dictionary
    // If it exists, return err::TABLE_NAME_EXISTS
    //
    // Create a RBF
    // Update the catalog
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    // Delete the corresponding RBF
    // Do equality scan on catalog Table Table
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    // Use the catalog to find the attributes of this table
    // Populate attrs with proper types
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    // Use catalog to get filename corresponding tableName
    // Open filehanlde to that file
    // Use RBFM to insert data, store RID into rid
    return -1;
}

RC RelationManager::deleteTuples(const string &tableName)
{
    // Use catalog to get filename
    // Open file
    // Call deleteRecords
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    // Use catalog to get filename
    // Open file
    // Call delete record on that RID
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    // Use catalog to get filename
    // Open file
    // Call update record on that RID
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    // Use catalog to get filename
    // Open file
    // Call read record on that RID
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    // Use catalog to get filename
    // Open file
    // Call readAttribute on that RID
    return -1;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    // Use catalog to get filename
    // Open file
    // Call reorganize page
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Use catalog to get filename
    // Open file
    // Call call scan 
    return -1;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    return -1;
}
