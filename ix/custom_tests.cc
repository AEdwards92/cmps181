#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_1(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Delete a few entries
    // 5. Try to delete a deleted entry
    // 6. Close Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 1****" << endl;

    RID rid;
    RC rc;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key;
    int inRidPageNumSum = 0;
    unsigned numOfTuples = 1000;

    // create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    // open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File, " << indexFileName << " Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_return;
    }

    // insert entry
    for(unsigned i = 0; i <= numOfTuples; i++)
    {
        key = i+1;//just in case somebody starts pageNum and recordId from 1
        rid.pageNum = key;
        rid.slotNum = key+1;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
            goto error_close_index;
        }
        inRidPageNumSum += rid.pageNum;
    }

    for (int times = 1; times <= 2; times++){
        for(unsigned i = numOfTuples; i >= numOfTuples - 2; i--)
        {
            key = i+1;
            rid.pageNum = key;
            rid.slotNum = key+1;

            rc = indexManager->deleteEntry(fileHandle, attribute, &key, rid);
            if(rc != success and times == 1)
            {
                cout << "Failed Deleting Entry..." << endl;
                goto error_close_index;
            }
            inRidPageNumSum -= key;
        }
    }

    cout << "Handles basic deleting test" << endl;

    // Close Index
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
    }

    return success;

error_close_index: //close index
	indexManager->closeFile(fileHandle);

error_return:
	return fail;
}

void test()
{
	const string indexAgeFileName = "Age_idx";
	Attribute attrAge;
	attrAge.length = 4;
	attrAge.name = "Age";
	attrAge.type = TypeInt;

    testCase_1(indexAgeFileName, attrAge);
    return;
}

int main()
{
    //Global Initializations
    cout << "****Starting Test Cases****" << endl;
    indexManager = IndexManager::instance();
    test();
    return 0;
}

