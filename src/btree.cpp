/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string &relationName,
					   std::string &outIndexName,
					   BufMgr *bufMgrIn,
					   const int attrByteOffset,
					   const Datatype attrType)
{
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	// Check to see if the corresponding index file exists. If so, open the file.
	// If not, create it
	try
	{
		BlobFile::open(relationName);
	}
	catch (FileNotFoundException &e)
	{
		file = new BlobFile(outIndexName, true);
		Page *headerPage;
		bufMgrIn->allocPage(file, headerPageNum, headerPage);
		// insert metadata in header page
		IndexMetaInfo *metadata = (IndexMetaInfo *)headerPage;

		strcpy(metadata->relationName, relationName.c_str());
		metadata->attrByteOffset = attrByteOffset;
		metadata->attrType = attrType;
		metadata->rootPageNo = rootPageNum;

		Page *rootPage;
		bufMgrIn->allocPage(file, rootPageNum, rootPage);

		// insert entries for every tuple in the base relation using FileScan class.
		FileScan *scanner = new FileScan(relationName, bufMgrIn);
		std::string currRecord = scanner->getRecord();

		// loop through every record in the file that contains the the relation (relationName)
		//   while() {

		//  }
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
