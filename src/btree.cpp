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

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

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
	// end current scan and get ready to start a new scan
	if (scanExecuting == true) {
		endScan();
	}
	// BadOpcodesException takes higher precedence over BadScanrangeException
	// only support GT and GTE here
	if (lowOpParm != GT || lowOpParm != GTE) {
		throw BadOpcodesException();
	}
	// only support LT and LTE here
	if (highOpParm != LT || highOpParm != LTE) {
		throw BadOpcodesException();
	}

	// If lowValue > highValue, throw the exception BadScanrangeException.
	if (lowOpParm > highOpParm) {
		throw BadScanrangeException();
	}

	// Both the high and low values are in a binary form, i.e., for integer
	// keys, these point to the address of an integer.

	// Start from root to find out the leaf page that contains the first RecordID
	// that satisfies the scan parameters. Keep that page pinned in the buffer pool.
	currentPageNum = rootPageNum;
	bufMgr->readPage(file, rootPageNum, currentPageData);
	bufMgr->unPinPage(file, currentPageNum, true);
	



	// // If there is no key in the B+ tree that satisfies the scan criteria,
	// // throw the exception NoSuchKeyFoundException.
	// if () {
	// 	throw NoSuchKeyFoundException();
	// }
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
