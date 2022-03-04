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

namespace badgerdb {

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

		// initialize root
		LeafNodeInt *root = (LeafNodeInt *)rootPage;

		// insert entries for every tuple in the base relation using FileScan class.
		FileScan *scanner = new FileScan(relationName, bufMgrIn);
		std::string currRecord = scanner->getRecord();
		RecordId recordId;
		while (true)
		{
			try
			{
				scanner->scanNext(recordId);
				const char *key = currRecord.c_str() + attrByteOffset;
				insertEntry(key, recordId);
			}
			catch (EndOfFileException &e)
			{
				break;
			}
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
	scanExecuting = false;
  	bufMgr->flushFile(file);
  	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 * @param key			Key to insert, pointer to integer/double/char string
 * @param rid			Record ID of a record whose entry is getting inserted into the index.
 **/

void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{

	Page *root;
	bufMgr->readPage(file, rootPageNum, root);

	// If leaf node, make helper for insert in leaf Node
	// compare root page no to starting root page number, if true, then leaf else non-leaf node

	RIDKeyPair<int> pair;
	pair.set(rid, (*((int *)key)));
}

void BTreeIndex::insertToLeaf(LeafNodeInt *currNode, PageId pageid, RIDKeyPair<int> pair)
{

}

void BTreeIndex::insertToNonLeaf(NonLeafNodeInt *currNode, PageId pageid, RIDKeyPair<int> pair)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
 * Begin a filtered scan of the index.  For instance, if the method is called
 * using ("a",GT,"d",LTE) then we should seek all entries with a value
 * greater than "a" and less than or equal to "d".
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
 * @param lowVal	Low value of range, pointer to integer / double / char string
 * @param lowOp		Low operator (GT/GTE)
 * @param highVal	High value of range, pointer to integer / double / char string
 * @param highOp	High operator (LT/LTE)
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
 * @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
 **/

void BTreeIndex::startScan(const void *lowValParm,
						   const Operator lowOpParm,
						   const void *highValParm,
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
	if (scanExecuting == false) {
		throw ScanNotInitializedException();
	} else {
		scanExecuting == false;
		bufMgr->unPinPage(file, currentPageNum, false);
	}
  
}

}

