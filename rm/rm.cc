#include "rm.h"
#include "../util/errcodes.h"
#include <iostream>
#include <cstring>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance() {
	if (!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager() :
	TABLE_ID_COUNTER(1) {
	rbfm = RecordBasedFileManager::instance();
	Attribute attr;
	attr.name = "TableId";
	attr.type = TypeInt;
	attr.length = 4;
    tableVec.push_back(attr);

	attr.name = "TableName";
	attr.type = TypeVarChar;
	attr.length = 256;
	tableVec.push_back(attr);

	attr.name = "TableType";
	attr.type = TypeVarChar;
	attr.length = 256;
	tableVec.push_back(attr);

	attr.name = "FileName";
	attr.type = TypeVarChar;
	attr.length = 256;
	tableVec.push_back(attr);

	attr.name = "NumOfColumns";
	attr.type = TypeInt;
	attr.length = 4;
	tableVec.push_back(attr);

	attr.name = "TableId";
	attr.length = 4;
	attr.type = TypeInt;
	columnVec.push_back(attr);

	attr.name = "TableName";
	attr.length = 256;
	attr.type = TypeVarChar;
	columnVec.push_back(attr);

	attr.name = "ColumnName";
	attr.length = 256;
	attr.type = TypeVarChar;
	columnVec.push_back(attr);

	attr.name = "ColumnType";
	attr.length = 256;
	attr.type = TypeVarChar;
	columnVec.push_back(attr);

	attr.name = "ColumnPosition";
	attr.length = 4;
	attr.type = TypeInt;
	columnVec.push_back(attr);

	attr.name = "MaxLength";
	attr.length = 4;
	attr.type = TypeInt;
	columnVec.push_back(attr);

	if (fexist("Tables.tbl")) {
		loadSystem();
	} else {
		createTable("Tables", tableVec, "System");
		createTable("Columns", columnVec, "System");
	}

}

RC RelationManager::loadSystem() {

	RM_ScanIterator rmsi;
	vector < string > attributeNames;
	RID rid;

	attributeNames.push_back(tableVec[0].name); //table id
	attributeNames.push_back(tableVec[1].name); //table name

	scan("Tables", tableVec[0].name, NO_OP, NULL, attributeNames, rmsi);

	char * data = (char*) malloc(MAX_TABLE_RECORD_SIZE);
	char * beginOfData = data;

	int maxTableId = 0;
	int tableId = 0;
	while (rmsi.getNextTuple(rid, data) != RM_EOF) {
		memcpy(&tableId, data, sizeof(int));

		int tableNameLength;
		data = data + sizeof(int); //jump the length
		memcpy(&tableNameLength, data, sizeof(int));

		data = data + sizeof(int);
		string tableName = string(data, tableNameLength);

		if (tableId > maxTableId)
			maxTableId = tableId;

		map<int, RID> * tableIDToRidMap = new map<int, RID> ;
		(*tableIDToRidMap)[tableId] = rid;

		tablesMap[tableName] = tableIDToRidMap;
		data = beginOfData;
	}

	TABLE_ID_COUNTER = maxTableId + 1; //get the max and add 1
	rmsi.close();

	attributeNames.clear();
	attributeNames.push_back(columnVec[0].name); //table id
	attributeNames.push_back(columnVec[4].name); //column position
	scan("Columns", columnVec[0].name, NO_OP, NULL, attributeNames, rmsi);

	int columnPosition;
	while (rmsi.getNextTuple(rid, data) != RM_EOF) {
		memcpy(&tableId, data, sizeof(int));

		data = data + sizeof(int);
		memcpy(&columnPosition, data, sizeof(int));


		if (columnsMap.find(tableId) != columnsMap.end()) {
			(*columnsMap[tableId])[columnPosition] = rid;
		} else {
			map<int, RID> * colEntryMap = new map<int, RID> ();
			(*colEntryMap)[columnPosition] = rid;
			(columnsMap[tableId]) = colEntryMap;
		}

		data = beginOfData;
	}
	rmsi.close();

	attributeNames.clear();


	free(beginOfData);

	return 0;
}

RelationManager::~RelationManager() {
	_rm = NULL;

	for (map<string, map<int, RID> *>::iterator it = tablesMap.begin(); it
			!= tablesMap.end(); ++it) {
		delete it->second;
	}

	for (map<int, map<int, RID> *>::iterator it = columnsMap.begin(); it
			!= columnsMap.end(); ++it) {
		delete it->second;
	}

}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {
	//invalid table name
	if (tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0) {
		return -1;
	}

	return createTable(tableName, attrs, "user");

}

//createTable helper function
RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> & attrs, const string & type) {

	RC ret = -1;
	FileHandle fileHandle;
	string fileName = tableName + ".tbl";
	RID rid;

	if (fileName.compare("Columns.tbl") != 0) {
		ret = rbfm->createFile(fileName);
		if (ret != err::OK) {
			return ret;
		}
	}

	if (fileName.compare("Tables.tbl") == 0) {
		ret = rbfm->createFile("Columns.tbl");
		if (ret != err::OK) {
			return ret;
		}
	}

	ret = rbfm->openFile("Tables.tbl", fileHandle);
	if (ret != err::OK) {
		return ret;
	}

	int numOfCol = (int) attrs.size();
	ret = insertTablesEntry(tableName, type, fileName, fileHandle, numOfCol,
			rid);

	map<int, RID> * tableIDToRidMap = new map<int, RID> ;
	(*tableIDToRidMap)[TABLE_ID_COUNTER] = rid;
	tablesMap[tableName] = tableIDToRidMap;

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->closeFile(fileHandle);
	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->openFile("Columns.tbl", fileHandle);
	if (ret != err::OK) {
		return ret;
	}


	for (int i = 0; i < numOfCol; i++) {
		int columnPosition = i + 1;
		insertColumnsEntry(tableName, attrs[i].name, fileHandle,
				columnPosition, attrs[i].length, rid, attrs[i].type);
		populateColumnsMap(rid, columnPosition);
	}

	if (ret == err::OK) {
		TABLE_ID_COUNTER += 1;
	}
	return rbfm->closeFile(fileHandle);
}

void RelationManager::populateColumnsMap(RID &rid, int columnPosition) {
	if (columnsMap.find(TABLE_ID_COUNTER) != columnsMap.end()) {
		(*columnsMap[TABLE_ID_COUNTER])[columnPosition] = rid;
	} else {
		map<int, RID> * colEntryMap = new map<int, RID> ();
		(*colEntryMap)[columnPosition] = rid;
		(columnsMap[TABLE_ID_COUNTER]) = colEntryMap;
	}
}

RC RelationManager::insertTablesEntry(string tableName, string tableType,
		string fileName, FileHandle &fileHandle, int numOfCol, RID &rid) {
	RC ret;
	char * recordBuffer = (char*) malloc(determineMemoryNeeded(tableVec));

	int offset = 0;
	appendData(tableVec[0].length, offset, recordBuffer,
			(char*) &TABLE_ID_COUNTER, tableVec[0].type);
	appendData((int) tableName.size(), offset, recordBuffer, tableName.c_str(),
			tableVec[1].type);
	appendData((int) tableType.size(), offset, recordBuffer, tableType.c_str(),
			tableVec[2].type);
	appendData((int) fileName.size(), offset, recordBuffer, fileName.c_str(),
			tableVec[3].type);
	appendData(tableVec[4].length, offset, recordBuffer, (char*) &numOfCol,
			tableVec[4].type);

	ret = rbfm->insertRecord(fileHandle, tableVec, recordBuffer, rid);

	if (ret != err::OK) {
		return ret;
	}

	free(recordBuffer);

	return ret;
}

RC RelationManager::insertColumnsEntry(string tableName, string columnName,
		FileHandle &fileHandle, int colPosition, int maxLength, RID &rid,
		AttrType colType) {
	RC ret;
	int memorySize = determineMemoryNeeded(columnVec);
	char * recordBuffer = (char*) malloc(memorySize);

	int offset = 0;

	appendData(columnVec[0].length, offset, recordBuffer,
			(char*) &TABLE_ID_COUNTER, columnVec[0].type); //table_id
	appendData((int) tableName.size(), offset, recordBuffer, tableName.c_str(),
			columnVec[1].type); //table_name
	appendData((int) columnName.size(), offset, recordBuffer,
			columnName.c_str(), columnVec[2].type); //column_name

	string type;
	if (colType == TypeVarChar) {
		type = "var_char";
	} else if (colType == TypeInt) {
		type = "int";
	} else if (colType == TypeReal) {
		type = "real";
	}

	appendData((int) type.size(), offset, recordBuffer, type.c_str(),
			columnVec[3].type);
	appendData(columnVec[4].length, offset, recordBuffer, (char*) &colPosition,
			columnVec[4].type);
	appendData(columnVec[5].length, offset, recordBuffer, (char*) &maxLength,
			columnVec[5].type);

	ret = rbfm->insertRecord(fileHandle, columnVec, recordBuffer, rid);

	if (ret != err::OK) {
		return ret;
	}

	free(recordBuffer);

	return ret;
}

RC RelationManager::deleteTable(const string &tableName) {
	RC ret = -1;
	if (tablesMap.find(tableName) != tablesMap.end())
	{
		map<int, RID> * tableID = tablesMap[tableName];

		int table_ID = (*tableID).begin()->first;

		RID rid = (*tableID).begin()->second;
		if (isSystemTableRequest(tableName)) {
			return -1;
		}
		map<int, RID> * columnsEntries = columnsMap[table_ID];

		FileHandle fileHandle;
		ret = rbfm->openFile("Columns.tbl", fileHandle);

		if (ret == err::OK) {

			vector<Attribute> recordDescriptor;
			for (map<int, RID>::iterator it = (*columnsEntries).begin(); it
					!= (*columnsEntries).end(); ++it) {
				if (ret != err::OK) {
					break;
				}

				rid = it->second;
				ret = rbfm->deleteRecord(fileHandle, columnVec, rid);
			}

			ret = rbfm->closeFile(fileHandle);

			if (ret != err::OK) {
				return ret;
			}


			delete (columnsEntries);
			columnsMap.erase(table_ID);


			rid = (*tableID).begin()->second;

			ret = rbfm->openFile("Tables.tbl", fileHandle);

			if (ret != err::OK) {
				return ret;
			}

			ret = rbfm->deleteRecord(fileHandle, tableVec, rid);

			if (ret != err::OK) {
				rbfm->closeFile(fileHandle);
				return ret;
			}

			delete (tableID);
			tablesMap.erase(tableName);

			ret = rbfm->closeFile(fileHandle);

			if (ret != err::OK) {
				return ret;
			}

			string fileName = tableName + ".tbl";
			ret = rbfm->destroyFile(fileName);

			if (ret != err::OK) {
				return ret;
			}
		}
	}
	return ret;
}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	RC ret = -1;

	if (tablesMap.find(tableName) != tablesMap.end()) //check if table exists in the map
	{
		map<int, RID> * tableID = tablesMap[tableName];

		int table_ID = (*tableID).begin()->first;
		map<int, RID> * columnsEntries = columnsMap[table_ID];

		FileHandle fileHandle;
		ret = rbfm->openFile("Columns.tbl", fileHandle);

		if (ret != err::OK) {
			return ret;
		}

		RID rid;

		char * columnsRecord = (char*) malloc(MAX_COLUMNS_RECORD_SIZE);
		char * beginOfData = columnsRecord;

		for (map<int, RID>::iterator it = (*columnsEntries).begin(); it
				!= (*columnsEntries).end(); ++it) {
			columnsRecord = beginOfData;
			rid = it->second;

			Attribute attr;

			if (ret == err::OK) {
				rbfm->readRecord(fileHandle, columnVec, rid, columnsRecord);

				//read the column name
				columnsRecord = columnsRecord + sizeof(int); //skip the table_id

				int stringLength;
				memcpy(&stringLength, columnsRecord, sizeof(int));
				columnsRecord = columnsRecord + sizeof(int) + stringLength;

				memcpy(&stringLength, columnsRecord, sizeof(int));
				columnsRecord = columnsRecord + sizeof(int);

				attr.name = string(columnsRecord, stringLength);
				columnsRecord = columnsRecord + stringLength;

				memcpy(&stringLength, columnsRecord, sizeof(int));
				columnsRecord = columnsRecord + sizeof(int);

				string colType = string(columnsRecord, stringLength);

				if (colType.compare("var_char") == 0) {
					attr.type = TypeVarChar;
				} else if (colType.compare("int") == 0) {
					attr.type = TypeInt;
				} else {
					attr.type = TypeReal;
				}

				columnsRecord = columnsRecord + stringLength + sizeof(int);

				unsigned length;
				memcpy(&length, columnsRecord, sizeof(AttrLength));
				attr.length = length;

				attrs.push_back(attr);

			}
		}

		free(beginOfData);

		rbfm->closeFile(fileHandle);
	}
	return ret;

}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {
	if (isSystemTableRequest(tableName)) {
		return -1;
	}

	FileHandle fileHandle;

	vector<Attribute> tableAttribs;

	map<int, RID> * tableID = tablesMap[tableName];
	rid = (*tableID).begin()->second;

	getAttributes(tableName, tableAttribs);
	string fileName = tableName + ".tbl";

	RC ret;
	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->insertRecord(fileHandle, tableAttribs, data, rid);

	if (ret != err::OK) {
		rbfm->closeFile(fileHandle);
		return ret;
	}

	return rbfm->closeFile(fileHandle);

}

RC RelationManager::deleteTuples(const string &tableName) {
	if (isSystemTableRequest(tableName)) {
		return -1;
	}

	string fileName = tableName + ".tbl";
	FileHandle fileHandle;
	RC ret;
	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->deleteRecords(fileHandle);

	if (ret != err::OK) {
		rbfm->closeFile(fileHandle);
		return ret;
	}

	return rbfm->closeFile(fileHandle);

}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	if (isSystemTableRequest(tableName)) {
		return -1;
	}

	FileHandle fileHandle;
	RC ret;

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	string fileName = tableName + ".tbl";
	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
//	if (ret != err::OK) {
//		rbfm->closeFile(fileHandle);
//		return ret;
//	}

	return rbfm->closeFile(fileHandle);

}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	if (isSystemTableRequest(tableName)) {
		return -1;
	}

	FileHandle fileHandle;

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	string fileName = tableName + ".tbl";
	RC ret;
	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);

	if (ret != err::OK) {
		rbfm->closeFile(fileHandle);
		return ret;
	}

	return rbfm->closeFile(fileHandle);

}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	if (tablesMap.find(tableName) == tablesMap.end())
	{
		return -1;
	}

	FileHandle fileHandle;
	string fileName = tableName + ".tbl";
	vector<Attribute> recordDescriptor;

	RC ret;
	ret = getAttributes(tableName, recordDescriptor);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);

	if (ret != err::OK) {
		rbfm->closeFile(fileHandle);
		return ret;
	}

	return rbfm->closeFile(fileHandle);

}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {
	FileHandle fileHandle;
	string fileName = tableName + ".tbl";
	vector<Attribute> recordDescriptor;

	int ret = getAttributes(tableName, recordDescriptor);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName,
			data);

	if (ret != err::OK) {
		rbfm->closeFile(fileHandle);
		return ret;
	}

	return rbfm->closeFile(fileHandle);

}

RC RelationManager::reorganizePage(const string &tableName,
		const unsigned pageNumber) {
	FileHandle fileHandle;
	string fileName = tableName + ".tbl";
	RC ret;
	ret = rbfm->openFile(fileName, fileHandle);
	if (ret != err::OK) {
		return ret;
	}

	vector<Attribute> recordDescriptor;

	ret = getAttributes(tableName, recordDescriptor);

	if (ret != err::OK) {
		rbfm->closeFile(fileHandle);
		return ret;
	}

	ret = rbfm->reorganizePage(fileHandle, recordDescriptor, pageNumber);

	if (ret != err::OK) {
		ret = rbfm->closeFile(fileHandle);
		return ret;
	}

	return rbfm->closeFile(fileHandle);

}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {
	string fileName = tableName + ".tbl";

	RC ret;
	ret = rbfm->openFile(fileName, rm_ScanIterator.fileHandle);
	if (ret != err::OK) {
		return ret;
	}

	if (tableName.compare("Tables") == 0)
		return rm_ScanIterator.initialize(tableVec, compOp, value,
				attributeNames, conditionAttribute);
	else if (tableName.compare("Columns") == 0)
		return rm_ScanIterator.initialize(columnVec, compOp, value,
				attributeNames, conditionAttribute);
	else {
		vector<Attribute> recordDescriptor;
		ret = getAttributes(tableName, recordDescriptor);
		if (ret != err::OK) {
			return ret;
		}
		return rm_ScanIterator.initialize(recordDescriptor, compOp, value,
				attributeNames, conditionAttribute);
	}

}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
	return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr) {
	return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName) {
	return -1;
}

void RelationManager::appendData(int fieldLength, int &offset,
		char * pageBuffer, const char * dataToWrite, AttrType attrType) {

	if (attrType == TypeVarChar) {
		memcpy(pageBuffer + offset, &fieldLength, sizeof(int));
		offset += sizeof(int);
	}

	memcpy(pageBuffer + offset, dataToWrite, fieldLength);
	offset += fieldLength;
}

short RelationManager::determineMemoryNeeded(
		const vector<Attribute> &attributes) {
	short size = 0;

	for (int i = 0; i < (int) attributes.size(); i++) {
		if (attributes[i].type == TypeVarChar) {
			size += sizeof(int);
		}

		size += attributes[i].length;
	}

	return size;
}

bool RelationManager::isSystemTableRequest(string tableName) {
	return false;
	bool isSysTbl = false;

	map<int, RID> * tableID = tablesMap[tableName];


	RID rid = (*tableID).begin()->second;

	char * tableType = (char*) malloc(sizeof(int) + tableVec[2].length);
	char * data = tableType;

	readAttribute(tableName, rid, "TableType", tableType);

	int tblTypeLength;
	memcpy(&tblTypeLength, tableType, sizeof(int));

	tableType = tableType + sizeof(int);

	string tbltype = string(tableType, tblTypeLength);

	if (tbltype.compare("System") == 0) {
		isSysTbl = true;
	}

	free(data);

	return isSysTbl;
}

bool RelationManager::fexist(string filename) {
	struct stat buffer;
	if (stat(filename.c_str(), &buffer)) {
		return false;
	}
	return true;
}

RM_ScanIterator::RM_ScanIterator() {
	rbfm = RecordBasedFileManager::instance();
}

RM_ScanIterator::~RM_ScanIterator() {
}

RC RM_ScanIterator::initialize(const vector<Attribute> &recordDescriptor,
		const CompOp compOp, const void *value,
		const vector<string> &attributeNames, const string &conditionAttribute) {
	return rbfm_scanner.init(fileHandle, recordDescriptor, conditionAttribute, compOp, value,
			attributeNames);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return rbfm_scanner.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
	rbfm_scanner.close();
	return rbfm->closeFile(fileHandle);
}
