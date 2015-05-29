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
	ix = IndexManager::instance();
	pfm = PagedFileManager::instance();

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

	//added for index
	attr.name = "TableID";
	attr.length = 4;
	attr.type = TypeInt;
	indexVec.push_back(attr);

	attr.name = "TableName";
	attr.length = 256;
	attr.type = TypeVarChar;
	indexVec.push_back(attr);

	attr.name = "ColumnPosition";
	attr.length = 4;
	attr.type = TypeInt;
	indexVec.push_back(attr);

	attr.name = "ColumnName";
	attr.length = 256;
	attr.type = TypeVarChar;
	indexVec.push_back(attr);

	if (fexist("Tables.tbl")) {
		loadSystem();
	} else {
		createTable("Tables", tableVec, "System");
		createTable("Columns", columnVec, "System");
		createTable("Indices", indexVec, "System");
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

		map<int, RID> * tableIDToRidMap = new map<int, RID> ();
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

	//added for index
	attributeNames.push_back(indexVec[0].name); //table id
	attributeNames.push_back(indexVec[2].name); //column position
	scan("Indices", indexVec[0].name, NO_OP, NULL, attributeNames, rmsi);
	while (rmsi.getNextTuple(rid, data) != RM_EOF) {
		memcpy(&tableId, data, sizeof(int));

		data = data + sizeof(int);
		memcpy(&columnPosition, data, sizeof(int));

		if (indexMap.find(tableId) != indexMap.end()) {
			(*indexMap[tableId])[columnPosition] = rid;
		} else {
			map<int, RID> * indexEntryMap = new map<int, RID> ();
			(*indexEntryMap)[columnPosition] = rid;
			(indexMap[tableId]) = indexEntryMap;
		}

		data = beginOfData;
	}
	rmsi.close();

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

	for (map<int, map<int, RID> *>::iterator it = indexMap.begin(); it
			!= indexMap.end(); ++it) {
		delete it->second;
	}

}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {
	//invalid table name
	if (tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0
			|| tableName.compare("Indices") == 0) {
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
		free(recordBuffer);
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

	//	if (ret != err::OK) {
	//		return ret;
	//	}

	free(recordBuffer);

	return ret;
}

RC RelationManager::insertIndexEntry(string tableName, string columnName,
		int tableID, int columnPos, FileHandle &fileHandle, RID &rid) {
	char * recordBuffer = (char *) malloc(determineMemoryNeeded(indexVec));

	RC ret;
	int offset = 0;

	appendData(indexVec[0].length, offset, recordBuffer, (char *) &tableID,
			indexVec[0].type); //table id
	appendData(tableName.size(), offset, recordBuffer, tableName.c_str(),
			indexVec[1].type); //table name
	appendData(indexVec[2].length, offset, recordBuffer, (char *) &columnPos,
			indexVec[2].type); // column Position
	appendData(columnName.size(), offset, recordBuffer, columnName.c_str(),
			indexVec[3].type); // column name

	ret = rbfm->insertRecord(fileHandle, indexVec, recordBuffer, rid);
	//	if (ret != err::OK) {
	//		return ret;
	//	}

	free(recordBuffer);

	return ret;
}

RC RelationManager::deleteTable(const string &tableName) {
	RC ret = -1;
	if (isSystemTableRequest(tableName)) {
		return ret;
	}
	if (tablesMap.find(tableName) != tablesMap.end()) {
		map<int, RID> * tableID = tablesMap[tableName];

		int table_ID = (*tableID).begin()->first;

		RID rid;
		FileHandle fileHandle;

		//added for index

		vector<Attribute> recordDescriptor;
		ret = getAttributes(tableName, recordDescriptor);

		if (ret != err::OK)
			return ret;

		if (indexMap.find(table_ID) != indexMap.end()) {
			map<int, RID> * indexEntry = indexMap[table_ID];

			ret = rbfm->openFile("Indices.tbl", fileHandle);

			if (ret != err::OK) {
				return ret;
			}

			for (map<int, RID>::iterator itr = indexEntry->begin(); itr
					!= indexEntry->end(); itr++) {
				int position = itr->first;
				rid = itr->second;

				string columnName = recordDescriptor[position - 1].name;
				string indexFileName = tableName + "_" + columnName + ".idx";

				ret = ix->destroyFile(indexFileName);
				if (ret != err::OK) {
					rbfm->closeFile(fileHandle);
					return ret;
				}
				ret = rbfm->deleteRecord(fileHandle, indexVec, rid);
				if (ret != err::OK) {
					rbfm->closeFile(fileHandle);
					return ret;
				}

			}

			delete (indexEntry);
			indexMap.erase(table_ID);

			ret = rbfm->closeFile(fileHandle);
			if (ret != err::OK) {
				return ret;
			}
		}

		map<int, RID> * columnsEntries = columnsMap[table_ID];
		ret = rbfm->openFile("Columns.tbl", fileHandle);
		if (ret != err::OK) {
			rbfm->closeFile(fileHandle);
			return ret;
		}

		//vector < Attribute > recordDescriptor;
		for (map<int, RID>::iterator it = (*columnsEntries).begin(); it
				!= (*columnsEntries).end(); ++it) {
			rid = it->second;
			ret = rbfm->deleteRecord(fileHandle, columnVec, rid);
			if (ret != err::OK) {
				rbfm->closeFile(fileHandle);
				return ret;
			}
		}

		delete (columnsEntries);
		columnsMap.erase(table_ID);
		ret = rbfm->closeFile(fileHandle);
		if (ret != err::OK) {
			return ret;
		}
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

		return ret;
	}
	return ret;
}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	RC ret = -1;
	attrs.clear();

	if (tablesMap.find(tableName) != tablesMap.end())
	{
		map<int, RID> * tableID = tablesMap[tableName];

		int table_ID = (*tableID).begin()->first;
		map<int, RID> * columnsEntries = columnsMap[table_ID];

		FileHandle fileHandle;
		ret = rbfm->openFile("Columns.tbl", fileHandle);

		if (ret != err::OK) {
			rbfm->closeFile(fileHandle);
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

	vector<Attribute> recordDescriptor;

	map<int, RID> * tableID = tablesMap[tableName];
	int table_ID = tableID->begin()->first;
	//rid = (*tableID).begin()->second;

	getAttributes(tableName, recordDescriptor);
	string fileName = tableName + ".tbl";

	RC ret;
	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	ret = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);

	if (ret != err::OK) {
		rbfm->closeFile(fileHandle);
		return ret;
	}

	ret = rbfm->closeFile(fileHandle);
	if (ret != err::OK) {
		return ret;
	}

	//added for index
	if (indexMap.find(table_ID) == indexMap.end())
		return ret;

	for (map<int, RID>::iterator itr = indexMap[table_ID]->begin(); itr
			!= indexMap[table_ID]->end(); ++itr) {
		int position = itr->first;
		Attribute keyAttribute = recordDescriptor[position - 1];

		string indexFileName = tableName + "_" + keyAttribute.name + ".idx";
		FileHandle indexFileHandle;
		ret = ix->openFile(indexFileName, indexFileHandle);
		if (ret != err::OK) {
			return ret;
		}

		int startOffset = readFieldOffset(data, position, recordDescriptor);

		ret = ix->insertEntry(indexFileHandle, keyAttribute, (char *) data
				+ startOffset, rid);
		if (ret != err::OK) {
			ix->closeFile(indexFileHandle);
			return ret;
		}

		ret = ix->closeFile(indexFileHandle);
		if (ret != err::OK) {
			return ret;
		}
	}

	return ret;

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

	ret = rbfm->closeFile(fileHandle);
	if (ret != err::OK) {
		return ret;
	}

	//added for index
	int table_ID = tablesMap[tableName]->begin()->first;

	if (indexMap.find(table_ID) == indexMap.end())
		return ret;

	vector<Attribute> recordDescriptor;
	ret = getAttributes(tableName, recordDescriptor);
	if (ret != err::OK) {
		return ret;
	}

	map<int, RID> *indexEntry = indexMap[table_ID];
	for (map<int, RID>::iterator itr = indexEntry->begin(); itr
			!= indexEntry->end(); itr++) {
		int position = itr->first;
		string columnName = recordDescriptor[position - 1].name;
		string fileName = tableName + "_" + columnName + ".idx";

		ret = ix->destroyFile(fileName);
		if (ret != err::OK) {
			return ret;
		}

		ret = ix->createFile(fileName);
		if (ret != err::OK) {
			return ret;
		}
	}

	return ret;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	if (isSystemTableRequest(tableName)) {
		return -1;
	}

	map<int, RID> * tableID = tablesMap[tableName];
	int table_ID = tableID->begin()->first;

	FileHandle fileHandle;
	RC ret;

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	string fileName = tableName + ".tbl";
	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	//the data is needed for deleting indexes
	void *data = malloc(PAGE_SIZE);
	ret = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);

	if (ret != err::OK) {
		free(data);
		rbfm->closeFile(fileHandle);
		return ret;
	}

	ret = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	if (ret != err::OK) {
		free(data);
		rbfm->closeFile(fileHandle);
		return ret;
	}

	ret = rbfm->closeFile(fileHandle);

	if (ret != err::OK) {
		free(data);
		return ret;
	}

	//added codes for index
	if (indexMap.find(table_ID) == indexMap.end()) {
		free(data);
		return ret;
	}

	for (map<int, RID>::iterator itr = indexMap[table_ID]->begin(); itr
			!= indexMap[table_ID]->end(); ++itr) {
		int position = itr->first;
		Attribute keyAttribute = recordDescriptor[position - 1];

		string indexFileName = tableName + "_" + keyAttribute.name + ".idx";
		FileHandle indexFileHandle;
		ret = ix->openFile(indexFileName, indexFileHandle);
		if (ret != err::OK) {
			free(data);
			return ret;
		}
		int startOffset = readFieldOffset(data, position, recordDescriptor);

		ret = ix->deleteEntry(indexFileHandle, keyAttribute, (char *) data
				+ startOffset, rid);
		if (ret != err::OK) {
			free(data);
			ix->closeFile(indexFileHandle);
			return ret;
		}
		ret = ix->closeFile(indexFileHandle);
		if (ret != err::OK) {
			free(data);
			return ret;
		}
	}

	free(data);

	return ret;

}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	if (isSystemTableRequest(tableName)) {
		return -1;
	}

	map<int, RID> * tableID = tablesMap[tableName];
	int table_ID = tableID->begin()->first;

	FileHandle fileHandle;

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	string fileName = tableName + ".tbl";
	RC ret;
	ret = rbfm->openFile(fileName, fileHandle);

	if (ret != err::OK) {
		return ret;
	}

	void *oldData = malloc(PAGE_SIZE);
	ret = rbfm->readRecord(fileHandle, recordDescriptor, rid, oldData);
	if (ret != err::OK) {
		free(oldData);
		rbfm->closeFile(fileHandle);
		return ret;
	}

	ret = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);

	if (ret != err::OK) {
		free(oldData);
		rbfm->closeFile(fileHandle);
		return ret;
	}

	ret = rbfm->closeFile(fileHandle);

	if (ret != err::OK) {
		free(oldData);
		return ret;
	}

	//added codes for index
	if (indexMap.find(table_ID) == indexMap.end()) {
		free(oldData);
		return ret;//check valid table_ID
	}

	for (map<int, RID>::iterator itr = indexMap[table_ID]->begin(); itr
			!= indexMap[table_ID]->end(); ++itr) {
		int position = itr->first;
		Attribute keyAttribute = recordDescriptor[position - 1];
		int oldKeyStartOffset = readFieldOffset(oldData, position,
				recordDescriptor);
		int newKeyStartOffset = readFieldOffset(oldData, position,
				recordDescriptor);

		if (!isFieldEqual((char *) oldData + oldKeyStartOffset, (char *) data
				+ newKeyStartOffset, keyAttribute.type)) {
			string indexFileName = tableName + "_" + keyAttribute.name + ".idx";
			FileHandle indexFileHandle;

			ret = ix->openFile(indexFileName, indexFileHandle);
			if (ret != err::OK) {
				free(oldData);
				return ret;
			}

			ret = ix->deleteEntry(indexFileHandle, keyAttribute,
					(char *) oldData + oldKeyStartOffset, rid);
			if (ret != err::OK) {
				free(oldData);
				ix->closeFile(indexFileHandle);
				return ret;
			}

			ret = ix->insertEntry(indexFileHandle, keyAttribute, (char *) data
					+ newKeyStartOffset, rid);
			if (ret != err::OK) {
				free(oldData);
				ix->closeFile(indexFileHandle);
				return ret;
			}

			ret = ix->closeFile(indexFileHandle);
			if (ret != err::OK) {
				free(oldData);
				return ret;
			}
		}

	}

	free(oldData);

	return ret;

}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	if (tablesMap.find(tableName) == tablesMap.end()) {
		return -1;
	}

	FileHandle fileHandle;
	string fileName = tableName + ".tbl";
	RC ret;

	vector<Attribute> recordDescriptor;

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

//added codes for index
RC RelationManager::createIndex(const string & tableName,
		const string & attributeName) {
	RC ret = -1;

	//string indexFileName = tableName + "_idx";

	string indexFileName = tableName + "_" + attributeName + ".idx";

	if (tablesMap.find(tableName) == tablesMap.end())
		return ret;

	map<int, RID> * tableIDMap = tablesMap[tableName];
	int table_ID = (*tableIDMap).begin()->first;

	vector<Attribute> recordDescriptor;
	ret = getAttributes(tableName, recordDescriptor);
	if (ret != err::OK) {
		return ret;
	}

	int attrPos = 1;
	for (; attrPos <= (int) recordDescriptor.size(); attrPos++) {
		if (recordDescriptor[attrPos - 1].name.compare(attributeName) == 0)
			break;
	}
	if (attrPos > (int) recordDescriptor.size())
		return -1;

	map<int, RID> *indexEntryMap;
	if (indexMap.find(table_ID) == indexMap.end()) {
		indexEntryMap = new map<int, RID> ();
		indexMap[table_ID] = indexEntryMap;
	} else {
		indexEntryMap = indexMap[table_ID];

		if (indexEntryMap->find(attrPos) != indexEntryMap->end()) {
			return -1;
		}
	}

	ret = ix->createFile(indexFileName);
	if (ret != err::OK) {
		return ret;
	}
	FileHandle fileHandle;
	ret = rbfm->openFile("Indices.tbl", fileHandle);
	if (ret != err::OK) {
		return ret;
	}

	RID indexRid;
	ret = insertIndexEntry(tableName, attributeName, table_ID, attrPos,
			fileHandle, indexRid);
	if (ret != err::OK) {
		rbfm->closeFile(fileHandle);
		return ret;
	}

	ret = rbfm->closeFile(fileHandle);
	if (ret != err::OK) {
		return ret;
	}
	(*indexEntryMap)[attrPos] = indexRid;

	FileHandle indexFileHandle;
	ret = ix->openFile(indexFileName, indexFileHandle);
	if (ret != err::OK) {
		return ret;
	}

	RM_ScanIterator rmsi;
	Attribute keyAttribute = recordDescriptor[attrPos - 1];
	vector < string > attributeNames;
	attributeNames.push_back(keyAttribute.name);

	ret = scan(tableName, keyAttribute.name, NO_OP, NULL, attributeNames, rmsi);
	if (ret != err::OK) {
		ix->closeFile(indexFileHandle);
		return ret;
	}
	char *data = (char *) malloc(MAX_ATTRIBUTE_LENGTH);
	RID rid;

	while (rmsi.getNextTuple(rid, data) != RM_EOF)
		ix->insertEntry(indexFileHandle, keyAttribute, data, rid);

	rmsi.close();
	free(data);

	ret = ix->closeFile(indexFileHandle);
	return ret;
}

RC RelationManager::destroyIndex(const string & tableName,
		const string &attributeName) {
	RC ret = -1;
	string indexFileName = tableName + "_" + attributeName + ".idx";
	map<int, RID> * tableIDMap = tablesMap[tableName];
	int table_ID = (*tableIDMap).begin()->first;
	vector<Attribute> attributes;
	ret = getAttributes(tableName, attributes);
	if (ret != err::OK) {
		return ret;
	}
	int attrPos = 1;
	for (; attrPos <= (int) attributes.size(); attrPos++) {
		if (attributes[attrPos - 1].name.compare(attributeName) == 0)
			break;
	}
	if (attrPos > (int) attributes.size())
		return -1;
	if (indexMap.find(table_ID) == indexMap.end())
		return -1;

	map<int, RID> *indexEntryMap = indexMap[table_ID];

	if (indexEntryMap->find(attrPos) == indexEntryMap->end())
		return -1;
	RID indexRid = (*indexEntryMap)[attrPos];

	indexEntryMap->erase(attrPos);
	if (indexEntryMap->size() == 0) {
		delete (indexMap[table_ID]);
		indexMap.erase(table_ID);
	}

	ret = deleteTuple("Indices", indexRid);
	if (ret != err::OK) {
		return ret;
	}
	return ix->destroyFile(indexFileName);

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

RC RelationManager::indexScan(const string &tableName,
		const string &attributeName, const void *lowKey, const void *highKey,
		bool lowKeyInclusive, bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator) {
	RC ret = -1;
	string indexFileName = tableName + "_" + attributeName + ".idx";

	if (!fexist(indexFileName))
		return ret;
	ret = ix->openFile(indexFileName, rm_IndexScanIterator.indexFileHandle);
	if (ret != err::OK) {
		return ret;
	}
	Attribute keyAttribute;
	vector<Attribute> attributes;
	ret = getAttributes(tableName, attributes);
	if (ret != err::OK) {
		return ret;
	}
	unsigned i = 0;
	for (; i < attributes.size(); i++) {
		if (attributes[i].name.compare(attributeName) == 0) {
			keyAttribute = attributes[i];
			break;
		}
	}
	if (i == attributes.size()) {
		ix->closeFile(rm_IndexScanIterator.indexFileHandle);
		return -1;
	}

	return rm_IndexScanIterator.initialize(keyAttribute, lowKey, highKey,
			lowKeyInclusive, highKeyInclusive);

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

int RelationManager::readFieldOffset(const void *data, int attrPosition,
		vector<Attribute> recordDescriptor) {
	int offset = 0;

	for (int i = 0; i < attrPosition - 1; i++) {
		Attribute attr = recordDescriptor[i];

		if (attr.type == TypeInt)
			offset += sizeof(int);
		else if (attr.type == TypeReal)
			offset += sizeof(float);
		else {
			int fieldLength = *(int *) ((char *) data + offset);
			offset += sizeof(int) + fieldLength;
		}
	}

	return offset;
}

bool RelationManager::isFieldEqual(const char *a, const char *b, AttrType type) {
	if (type == TypeInt) {
		int A = *(int *) a;
		int B = *(int *) b;

		return A == B;
	} else if (type == TypeReal) {
		float A = *(float *) a;
		float B = *(float *) b;

		if (A - B < 0.00001 && A - B > -0.00001)
			return true;
		else
			return false;
	} else {
		int lengthA = *(int *) a;
		int lengthB = *(int *) b;
		string A(a + sizeof(int), lengthA);
		string B(b + sizeof(int), lengthB);

		return A.compare(B) == 0;
	}
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
	return rbfm_scanner.init(fileHandle, recordDescriptor, conditionAttribute,
			compOp, value, attributeNames);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return rbfm_scanner.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
	rbfm_scanner.close();
	return rbfm->closeFile(fileHandle);
}

RM_IndexScanIterator::RM_IndexScanIterator() {
	ix = IndexManager::instance();
}

RM_IndexScanIterator::~RM_IndexScanIterator() {
}

RC RM_IndexScanIterator::initialize(const Attribute keyAttribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive) {
	return ix->scan(indexFileHandle, keyAttribute, lowKey, highKey,
			lowKeyInclusive, highKeyInclusive, ix_scanner);
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	return ix_scanner.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close() {
	RC ret;
	ret = ix_scanner.close();

	if (ret != err::OK) {
		ix->closeFile(indexFileHandle);
		return ret;
	}

	return ix->closeFile(indexFileHandle);
}
