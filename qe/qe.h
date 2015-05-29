#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <float.h>
#include <limits.h>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)
using namespace std;

typedef enum {
	MIN = 0, MAX, SUM, AVG, COUNT
} AggregateOp;

struct Value {
	AttrType type;
	void *data;
};

struct Condition {
	string lhsAttr; // left-hand side attribute
	CompOp op; // comparison operator
	bool bRhsIsAttr; // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
	string rhsAttr; // right-hand side attribute if bRhsIsAttr = TRUE
	Value rhsValue; // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator {
public:
	virtual RC getNextTuple(void *data) = 0;
	virtual void getAttributes(vector<Attribute> &attrs) const = 0;
	virtual ~Iterator() {
	}
	;
};

class TableScan: public Iterator {

public:
	RelationManager &rm;
	RM_ScanIterator *iter;
	string tableName;
	string originalTableName;
	vector<Attribute> attrs;
	vector<string> attrNames;
	RID rid;

	TableScan(RelationManager &rm, const string &tableName, const char *alias =
			NULL) :
		rm(rm) {

		this->tableName = tableName;
		this->originalTableName = tableName;
		rm.getAttributes(tableName, attrs);
		unsigned i;
		for (i = 0; i < attrs.size(); ++i) {
			attrNames.push_back(attrs[i].name);
		}

		iter = new RM_ScanIterator();
		rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

		if (alias)
			this->tableName = alias;
	}
	;

	void setIterator() {
		iter->close();
		delete iter;
		iter = new RM_ScanIterator();
		rm.scan(originalTableName, "", NO_OP, NULL, attrNames, *iter);
	}
	;

	RC getNextTuple(void *data) {
		return iter->getNextTuple(rid, data);
	}
	;

	void getAttributes(vector<Attribute> &attrs) const {
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		for (i = 0; i < attrs.size(); ++i) {
			string tmp = tableName;
			tmp += ".";
			tmp += attrs[i].name;
			attrs[i].name = tmp;
		}
	}
	;

	~TableScan() {
		iter->close();
	}
	;
};

class IndexScan: public Iterator {

public:
	RelationManager &rm;
	RM_IndexScanIterator *iter;
	string tableName;
	string originalTableName;
	string attrName;
	vector<Attribute> attrs;
	char key[PAGE_SIZE];
	RID rid;

	IndexScan(RelationManager &rm, const string &tableName,
			const string &attrName, const char *alias = NULL) :
		rm(rm) {
		this->tableName = tableName;
		this->originalTableName = tableName;
		this->attrName = attrName;

		rm.getAttributes(tableName, attrs);

		iter = new RM_IndexScanIterator();
		rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

		if (alias)
			this->tableName = alias;
	}
	;

	void setIterator(void* lowKey, void* highKey, bool lowKeyInclusive,
			bool highKeyInclusive) {
		iter->close();
		delete iter;
		iter = new RM_IndexScanIterator();
		rm.indexScan(originalTableName, attrName, lowKey, highKey,
				lowKeyInclusive, highKeyInclusive, *iter);
	}
	;

	RC getNextTuple(void *data) {
		int rc = iter->getNextEntry(rid, key);
		if (rc == 0) {
			rc = rm.readTuple(tableName.c_str(), rid, data);
		}
		return rc;
	}
	;

	void getAttributes(vector<Attribute> &attrs) const {
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		for (i = 0; i < attrs.size(); ++i) {
			string tmp = tableName;
			tmp += ".";
			tmp += attrs[i].name;
			attrs[i].name = tmp;
		}
	}
	;

	~IndexScan() {
		iter->close();
	}
	;
};

class Filter: public Iterator {

public:
	Filter(Iterator *input, const Condition &condition);
	~Filter();

	RC getNextTuple(void *data);
	void getAttributes(vector<Attribute> &attrs) const;

private:
	Iterator *itr;
	void *value;
	void *condition;
	AttrType conditionType;
	unsigned attrPos;
	vector<Attribute> attrs;
	CompOp op;
};

class Project: public Iterator {
public:
	Project(Iterator *input, const vector<string> &attrNames);
	~Project();

	RC getNextTuple(void *data);
	void getAttributes(vector<Attribute> &attrs) const;

private:
	Iterator *itr;
	vector<Attribute> attrs;
	vector<Attribute> oriAttrs;
	void *tuple;

	void projectFields(const void *input, void *data,
			vector<Attribute> inputAttrs, vector<Attribute> attrs);
};

class NLJoin: public Iterator {
public:
	NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition,
			const unsigned numPages);
	~NLJoin();

	RC getNextTuple(void *data);
	void getAttributes(vector<Attribute> &attrs) const;

private:
	Iterator *leftItr;
	TableScan *rightItr;

	void *leftValue;
	void *rightValue;
	void *leftTuple;
	void *rightTuple;

	CompOp op;
	AttrType type;

	unsigned leftAttrPos;
	unsigned rightAttrPos;

	vector<Attribute> attrs;
	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;

	RC isEnd;
};

class INLJoin: public Iterator {
public:
	INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition,
			const unsigned numPages);

	~INLJoin();

	RC getNextTuple(void *data);
	void getAttributes(vector<Attribute> &attrs) const;

private:
	Iterator *leftItr;
	IndexScan *rightItr;

	void *leftValue;

	void *leftTuple;
	void *rightTuple;

	CompOp op;
	AttrType type;

	unsigned leftAttrPos;

	vector<Attribute> attrs;
	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;

	RC isEnd;
	bool leftHalf; // for NE_OP;

	void setCondition(CompOp op, void **lowKey, void **highKey,
			bool &lowKeyInclusive, bool &highKeyInclusive);
};

class Aggregate: public Iterator {

public:
	Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op);

	// Extra Credit
	Aggregate(Iterator *input, // Iterator of input R
			Attribute aggAttr, // The attribute over which we are computing an aggregate
			Attribute gAttr, // The attribute over which we are grouping the tuples
			AggregateOp op // Aggregate operation
			);

	~Aggregate() {
	}
	;

	RC getNextTuple(void *data);
	void getAttributes(vector<Attribute> &attrs) const;

private:
	short attrPos;
	int max_tuple_size;

	Iterator *itr;
	AggregateOp op;
	Attribute aggrAttribute;
	AttrType type;
	vector<Attribute> tblAttributes;

	bool isNextTuple;

	RC getMin(void *data);
	RC getMax(void *data);
	RC getAvg(void *data);
	RC getCount(void *data);
	RC getSum(void *data);
};

#endif
