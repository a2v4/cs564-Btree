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
using namespace std;
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

	// initialize
	this->bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attributeType;

	this->currentPageData = new Page;
	this->currentPageNum = (PageId)-1;
	this->rootPageNum = (PageId)-1;
	this->headerPageNum = (PageId)-1;
	this->scanExecuting = false;
	this->nextEntry = -1;

	// Check to see if the corresponding index file exists. If so, open the file.
	// If not, create it
	if (BlobFile::exists(outIndexName) == true)
	{
		// File found, so use it
		file = new BlobFile(outIndexName, false);
		headerPageNum = file->getFirstPageNo();
		// init page to store in
		Page *headerPage;
		// get page by pageNum and store in headerPage, which should be a META INFO page
		bufMgr->readPage(file, headerPageNum, headerPage);
		IndexMetaInfo *metaInfoPage = (IndexMetaInfo *)headerPage;
		rootPageNum = metaInfoPage->rootPageNo;
		// unpin page, and its not dirty because we only READ from it
		bufMgr->unPinPage(file, headerPageNum, false);
	}
	else
	{
		// File not found, so create it
		file = new BlobFile(outIndexName, true);
		Page *headerPage;
		bufMgrIn->allocPage(file, headerPageNum, headerPage);
		// insert metadata in header page
		IndexMetaInfo *metaInfoPage = (IndexMetaInfo *)headerPage;

		// copy relationName into the metaInfoPage
		strcpy(metaInfoPage->relationName, relationName.c_str());
		metaInfoPage->attrByteOffset = attrByteOffset;
		metaInfoPage->attrType = attrType;
		metaInfoPage->rootPageNo = rootPageNum;
		// save meta info in the headerPage
		memcpy(headerPage, metaInfoPage, sizeof(metaInfoPage));
		// unpin page, and mark dirty because we wrote the meta info to the header page
		bufMgr->unPinPage(file, headerPageNum, true);

		// init root page for use
		Page *rootPage;
		bufMgrIn->allocPage(file, rootPageNum, rootPage);

		// initialize root
		LeafNodeInt *root = (LeafNodeInt *)rootPage;
		currentPageNum = rootPageNum;
		currentPageData = rootPage;
		leafOccupancy = 0;
		
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
		// unpin page, and its not dirty because we only READ from it
		bufMgr->unPinPage(file, rootPageNum, false);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
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
	RIDKeyPair<int> pair;
	pair.set(rid, (*((int *)key)));

	// compare page number equal to starting root page number, if true, then leaf else non-leaf node
	// if leaf node, make helper for insert in leaf Node
	if (currentPageNum == rootPageNum)
	{
		// leafNode
		LeafNodeInt *currNode;
		insertToLeaf(currNode, currentPageNum, pair);
	}
	else
	{
		// non-leaf node
		NonLeafNodeInt *currNode;
		insertToNonLeaf(currNode, currentPageNum, pair);
	}
}


void BTreeIndex::insertToLeaf(LeafNodeInt *currNode, PageId pageid, RIDKeyPair<int> pair) {
	if(leafOccupancy == INTARRAYLEAFSIZE) {
		splitLeaf(currNode, pageid, pair);
	}
	else
	{
		// insert into available page in node
		int i = 0;
		while(i < leafOccupancy && currNode->keyArray[i] < pair.key) {
			i++;
		}
		//shift all right values one place to the right
		for(int j = i + 1; j < INTARRAYLEAFSIZE; j++){
			currNode->keyArray[j] = currNode->keyArray[j - 1];
		}
		currNode->keyArray[i] = pair.key;
		currNode->ridArray[i] = pair.rid;
		leafOccupancy++;
	}
	currentPageNum = pageid;
}


void BTreeIndex::insertToNonLeaf(NonLeafNodeInt *currNode, PageId pageid, int key) {
	if(nodeOccupancy == INTARRAYNONLEAFSIZE) {
		splitNonLeaf(currNode, pageid, key);
	} else {
		
		// insert into available page in node
		int i = 0;
		while(i < nodeOccupancy && currNode->keyArray[i] < key) {
			i++;
		}
		//shift all right values one place to the right
		for(int j = i + 1; j < INTARRAYNONLEAFSIZE; j++){
			currNode->keyArray[j] = currNode->keyArray[j - 1];
		}
		currNode->keyArray[i] = key;
		nodeOccupancy++;
	}
}


void BTreeIndex::splitLeaf(LeafNodeInt *currNode, PageId pageid, RIDKeyPair<int> pair) {
	//create new leafNode
	LeafNodeInt *newNode;

	// copy half the keys from previous node to this one and insert new one in ascending order
	bool insertedNewEntry = false;
	int i = leafOccupancy / 2;
	while(i < INTARRAYLEAFSIZE) {
		if(insertedNewEntry) {
			break;
		}
		if(pair.key <= currNode->keyArray[i]){
			newNode->keyArray[i] = pair.key;
			newNode->ridArray[i] = pair.rid;
			insertedNewEntry = true;
		}
		else
		{
			newNode->keyArray[i] = currNode->keyArray[i];
			newNode->ridArray[i] = currNode->ridArray[i];
		}
		i++;
	}
	// create new root which will be a Non leaf node
	NonLeafNodeInt *newInternalNode;
	nodeOccupancy = 0;
	//alloc new page for new non leaf node
	Page *newPage;
	File *newFile;
	PageId newPageId;
	bufMgr->allocPage(newFile, newPageId, newPage);

	//connect curr node to new leaf node
	currNode->rightSibPageNo = newPageId;

	// copy up leftmost key on new node up to the root
	insertToNonLeaf(newInternalNode, newPageId, pair.key);
}

void BTreeIndex::splitNonLeaf(NonLeafNodeInt *currNode, PageId pageid, int key) {
	//create new non leafNode
	NonLeafNodeInt *newNode;
	//copy half the keys from previous node to this one	
	
	bool insertedNewEntry = false;
	int i = nodeOccupancy / 2;
	while(i < INTARRAYLEAFSIZE) {
		if(insertedNewEntry) {
			break;
		}
		if(key <= currNode->keyArray[i]){
			newNode->keyArray[i] = key;
			insertedNewEntry = true;
		}
		else
		{
			newNode->keyArray[i] = currNode->keyArray[i];
		}
		i++;
	}

	//create new root which will be a Non leaf node
	NonLeafNodeInt *newInternalNode;
	nodeOccupancy = 0;

	//alloc new page for new non leaf node
	Page *newPage;
	File *newFile;
	PageId newPageId; 
	bufMgr->allocPage(newFile, newPageId, newPage);

	// copy up leftmost key on new node up to the root
	insertToNonLeaf(newInternalNode, newPageId, key);
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
	if (lowValParm > highValParm) {
		throw BadScanrangeException();
	}

	// Both the high and low values are in a binary form, i.e., for integer
	// keys, these point to the address of an integer.

	// Start from root to find out the leaf page that contains the first RecordID
	// that satisfies the scan parameters. Keep that page pinned in the buffer pool.
	//currentPageNum = rootPageNum;
	//bufMgr->readPage(file, rootPageNum, currentPageData);
	//bufMgr->unPinPage(file, currentPageNum, true);
	scanExecuting = true;
    bufMgr->readPage(file, rootPageNum, currentPageData);
    currentPageNum = rootPageNum;
    NonLeafNodeInt* currentNode = (NonLeafNodeInt *) currentPageData;

    while(currentNode-> level != 1){
    	//how to find the beginning of the range?
    	//(currentNode->keyArray[] > lowValParm  ?
    	PageID nextNodePageNum = currentNode->pageNoArray[first page?];
    	bufMgr->readPage(file, nextNodePageNum, currentPageData);
    	bufMgr->unPinPage(file, currentPageNum, false);
    	currentPageNum = nextNodePageNum;
    	
    	//go to next node
    	currentNode = (NonLeafNodeInt*) currentPageData;
    	
    }
    
    //while() looping until found
    	LeafNodeInt* currNode = (LeafNodeInt*) currentPageData;
    	for(int i = 0; i < leafOccupancy; i++) {
    		int key = currentNode->keyArray[i];
    		if((lowOpParm == GTE && highOpParm == LTE) && (key >= lowOpParm && key <= highOpParm))  {
    			scanExecuting = true;
    			loop = 1;
    			break;
    		} else if ((lowOpParm = GTE && highOpParm == LT) && (key >= lowOpParm && key < highOpParm)){
    			scanExecuting = true;
    			loop = 1;
    			break;			

    		} else if ((lowOpParm = GT && highOpParm == LTE) && (key > lowOpParm && key <= highOpParm)){
    			scanExecuting = true;
    			loop = 1;
    			break;		
    		} else if ((lowOpParm = GT && highOpParn == LT) && (key >= lowOpParm && key <= highOpParm)){
    			scanExecuting = true;
    			loop = 1;
    			break; 			
    		} else if ((highOpParm == LT and key >= highValParm) or (highOpParm == LTE and key >highValParm)){
    			bufMgr->unPinPage(file, currentPageNum, false);
    			throw NoSuchKeyFoundException();
    		} 
    		//when i is the last one and still not out of loop so its not found in the node
    		if (i == leafOccupancy - 1) {
    			bufMgr->unPinPage(file, currentPageNum, false);
    			if(currentNode->rightSibPageNo != 0){
    				currentPageNum = currentNode->rightSibPageNo;
    				bufMgr->readPage(file, currentPageNum, currentPageData)
    			} else {
    				throw NoSuchKeyFoundException();
    			}
    		}
    	}
    	if (loop == 1) {
    		break;
    	}
    }
    	
	



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
	if(!scanExecuting) {
    	throw ScanNotInitializedException();
  	}	
  	bufMgr->readPage(file, currentPageNum, currentPageData);
  	LeafNodeInt *currentNode = (LeafNodeInt *) currentPageData;

  	if(currentNode->rightSibPageNo == 0){
  		throw index_scan_completed_exception
  	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan()
{
	if (scanExecuting == false)
	{
		// throws ScanNotInitializedException() when called before a successful startScan call.
		throw ScanNotInitializedException();
	}
	// terminates the current scan
	scanExecuting = false;
	
	// unpins all the pages that have been pinned for the purpose of the scan
	bufMgr->unPinPage(file, currentPageNum, false);

	// reset scan data to NULL
	currentPageData = nullptr;
	PageId nullPage = -1;
	currentPageNum = nullPage;
	nextEntry = -1;
}


