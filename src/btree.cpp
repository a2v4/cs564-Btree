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

		// initialize
		this->bufMgr = bufMgrIn;
		this->attrByteOffset = attrByteOffset;
		this->attributeType = attrType;

		this->currentPageData = new Page;
		this->currentPageNum = Page::INVALID_NUMBER;
		this->rootPageNum = Page::INVALID_NUMBER;
		this->headerPageNum = Page::INVALID_NUMBER;
		this->scanExecuting = false;
		this->nextEntry = -1;

		if (this->attributeType == INTEGER)
		{
			this->nodeOccupancy = INTARRAYNONLEAFSIZE;
			this->leafOccupancy = INTARRAYLEAFSIZE;
		}

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

			// unpin page, and mark dirty because we wrote the meta info to the header page
			bufMgr->unPinPage(file, headerPageNum, true);

			// init root page for use
			Page *rootPage;
			bufMgrIn->allocPage(file, rootPageNum, rootPage);

			// initialize root
			NonLeafNodeInt *root = (NonLeafNodeInt *)rootPage;

			// insert entries for every tuple in the base relation using FileScan class.
			FileScan *scanner = new FileScan(relationName, bufMgrIn);
			std::string currRecord = scanner->getRecord();
			RecordId recordId;
			while (true)
			{
				try
				{
					scanner->scanNext(recordId);
					const char *currRecordStr = currRecord.c_str();
					// cast to INT to make key compatible in the future
					int *key = *((int *)(currRecordStr + attrByteOffset));
					insertEntry(key, recordId);
				}
				catch (EndOfFileException &e)
				{
					scanExecuting = false;
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
		// set up the RID-Key pair for insertion
		RIDKeyPair<int> pair;
		pair.set(rid, (*((int *)key)));

		Page *headerPage;
		bufMgr->readPage(file, headerPageNum, headerPage);
		IndexMetaInfo *metaInfo = (IndexMetaInfo *)headerPage;

		bool isRootALeaf = metaInfo->isRootALeaf;
		if (isRootALeaf == true)
		{
			// insertToLeaf(pair, rootPageNum)
		}
		else
		{
			insertToNonLeaf(pair, rootPageNum)
		}
		// Unpin and flush to disk
		bufMgr->unPinPage(file, rootPageNum, true);
	}

	/**
	 * @brief Insert given <rid, key> pair into a leaf at the given pageNo
	 *
	 * @param pair <rid, key> pair for insertion (only supporting INTEGER)
	 * @param pageNo page number where the leaf node is located
	 */
	void BTreeIndex::insertToLeaf(RIDKeyPair<int> pair, PageId pageNo)
	{
		Page *currPage;
		bufMgr->readPage(file, pageNo, currPage);
		LeafNodeInt *currLeafNode = (LeafNodeInt *)currPage;

		// used to find position in leaf to place
		int rid = pair.rid;
		int key = pair.key;

		int indexToInsertAt = 0;
		// make sure index is less than leaf occupancy limit
		// check if KEY is larger than the current one we are at
		// check if the page_number is valid for that entry at that index
		while (indexToInsertAt < leafOccupancy && key > currLeafNode->keyArray[indexToInsertAt] && currLeafNode->ridArray[indexToInsertAt].page_number != Page::INVALID_NUMBER)
		{
			// increment until we find a spot to insert our <rid, key> pair
			indexToInsertAt++;
		}

		// start checking every index afterwards to find the end
		int lastIndex = indexToInsertAt;
		while (lastIndex < leafOccupancy && currLeafNode->ridArray[lastIndex].page_number != Page::INVALID_NUMBER)
		{
			// increment till we find an OPEN spot in the arrays
			lastIndex++;
		}

		if (lastIndex < leafOccupancy)
		{
			// insert normally because this means there is room for our entry
			for (int i = lastIndex; indexToInsertAt < i; i--)
			{
				// move each element one index ahead to make room for our entry
				currLeafNode->keyArray[i] = currLeafNode->keyArray[i - 1];
				currLeafNode->ridArray[i] = currLeafNode->ridArray[i - 1];
			}
			// insert rid and key info into the respective arrays in the current leaf node
			currLeafNode->keyArray[indexToInsertAt] = key;
			currLeafNode->ridArray[indexToInsertAt] = rid;
		}
		else
		{
			// need to split leaf to make room
			// TODO: Utilize splitLeaf()
			// splitLeaf()
		}

		// Unpin and flush to disk
		bufMgr->unPinPage(file, pageNo, true);
	}

	void BTreeIndex::insertToNonLeaf(RIDKeyPair<int> pair, PageId pageNo)
	{
		Page *currPage;
		bufMgr->readPage(file, pageNo, currPage);
		NonLeafNodeInt *currNonLeafNode = (NonLeafNodeInt *)currPage;

		// used to find position in leaf to place
		int rid = pair.rid;
		int key = pair.key;

		int indexToInsertAt = 0;
		// make sure index is less than leaf occupancy limit
		// check if KEY is larger than the current one we are at
		// check if the page_number is valid for that entry at that index
		while (indexToInsertAt < leafOccupancy && key > currNonLeafNode->keyArray[indexToInsertAt] && currNonLeafNode->ridArray[indexToInsertAt].page_number != Page::INVALID_NUMBER)
		{
			// increment until we find a spot to insert our <rid, key> pair
			indexToInsertAt++;
		}

		// start checking every index afterwards to find the end
		int lastIndex = indexToInsertAt;
		while (lastIndex < leafOccupancy && currNonLeafNode->ridArray[lastIndex].page_number != Page::INVALID_NUMBER)
		{
			// increment till we find an OPEN spot in the arrays
			lastIndex++;
		}

		if (lastIndex < leafOccupancy)
		{
			// insert normally because this means there is room for our entry
			for (int i = lastIndex; indexToInsertAt < i; i--)
			{
				// move each element one index ahead to make room for our entry
				currNonLeafNode->keyArray[i] = currNonLeafNode->keyArray[i - 1];
				currNonLeafNode->ridArray[i] = currNonLeafNode->ridArray[i - 1];
			}
			// insert rid and key info into the respective arrays in the current leaf node
			currNonLeafNode->keyArray[indexToInsertAt] = key;
			currNonLeafNode->ridArray[indexToInsertAt] = rid;
		}
		else
		{
			// need to split leaf to make room
			// TODO: Utilize splitLeaf()
			// splitLeaf()
		}

		// Unpin and flush to disk
		bufMgr->unPinPage(file, pageNo, true);
	}

	void BTreeIndex::traverse(Page *currPage, RIDKeyPair<int> pair, int currLevel, bool isLeaf)
	{
		if (isLeaf == true)
		{
		}
	}

	void BTreeIndex::traverse(NonLeafNodeInt *root, RIDKeyPair<int> pair, int currLevel)
	{

		for (int i = nodeOccupancy - 1; i >= 0; i--)
		{
			if (pair.key > root->keyArray[i])
			{ // if key value > current key, insert at right of curr key
				if (isLeaf)
				{ // if next node leaf, insert
					Page *leaf;
					bufMgr->readPage(file, root->pageNoArray[i + 1], leaf);
					LeafNodeInt *leafNode = (LeafNodeInt *)leaf;
					insertToLeaf(leafNode, root->pageNoArray[i + 1], pair);
				}
				else // else traverse all children
				{
					for (int child = 0; child < nodeOccupancy + 1; i++)
					{
						Page *nonLeaf;
						bufMgr->readPage(file, root->pageNoArray[child], nonLeaf);
						NonLeafNodeInt *nonLeafNode = (NonLeafNodeInt *)nonLeaf;
						traverse(nonLeafNode, pair, currLevel + 1);
					}
				}
			}
			else if (pair.key <= root->keyArray[i])
			{ // if key value <= current key, insert at left of curr key
				if (isLeaf)
				{ // if next node leaf, insert
					Page *leaf;
					bufMgr->readPage(file, root->pageNoArray[i], leaf);
					LeafNodeInt *leafNode = (LeafNodeInt *)leaf;
					insertToLeaf(leafNode, root->pageNoArray[i], pair);
				}
				else // else traverse all children
				{
					for (int child = 0; child < nodeOccupancy + 1; i++)
					{
						Page *nonLeaf;
						bufMgr->readPage(file, root->pageNoArray[child], nonLeaf);
						NonLeafNodeInt *nonLeafNode = (NonLeafNodeInt *)nonLeaf;
						traverse(nonLeafNode, pair, currLevel + 1);
					}
				}
			}
		}
	}

	void BTreeIndex::sortedLeafEntry(LeafNodeInt *currNode, RIDKeyPair<int> pair, int occupancy)
	{
		// Insert new key in ascending order
		int i = 0;
		while (i < leafOccupancy && currNode->keyArray[i] < pair.key)
		{
			i++;
		}
		// shift all right values one place to the right
		for (int j = i + 1; j < leafOccupancy; j++)
		{
			currNode->keyArray[j] = currNode->keyArray[j - 1];
		}
		currNode->keyArray[i] = pair.key;
		currNode->ridArray[i] = pair.rid;
		occupancy++;
	}

	void BTreeIndex::sortedNonLeafEntry(NonLeafNodeInt *currNode, int key, int occupancy)
	{
		// insert into available page in node
		int i = 0;
		while (i < nodeOccupancy && currNode->keyArray[i] < key)
		{
			i++;
		}
		// shift all right values one place to the right
		for (int j = i + 1; j < nodeOccupancy; j++)
		{
			currNode->keyArray[j] = currNode->keyArray[j - 1];
		}
		currNode->keyArray[i] = key;
		occupancy++;
	}

	void BTreeIndex::insertToLeaf(LeafNodeInt *currNode, PageId pageid, RIDKeyPair<int> pair)
	{
		int occupancy = sizeof(currNode->keyArray) / sizeof(currNode->keyArray[0]);

		if (occupancy == leafOccupancy) // if leaf full
		{
			// split leaf
			splitLeaf(currNode, pageid, pair, occupancy);
			currNode->rightSibPageNo = pageid; // attach new node to current node
		}
		else
		{
			// else, insert into existing leaf
			sortedLeafEntry(currNode, pair, occupancy);
		}
	}

	void BTreeIndex::insertToNonLeaf(NonLeafNodeInt *currNode, PageId pageid, int key)
	{
		int occupancy = sizeof(currNode->keyArray) / sizeof(currNode->keyArray[0]);

		if (occupancy == nodeOccupancy) // if full
		{
			// split node
			splitNonLeaf(currNode, pageid, key, occupancy);
		}
		else
		{
			// else, insert into existing node
			sortedNonLeafEntry(currNode, key, occupancy);
		}
	}

	void BTreeIndex::splitLeaf(LeafNodeInt *currNode, PageId pageid, RIDKeyPair<int> pair, int occupancy)
	{

		// create new leafNode
		LeafNodeInt *newNode = new LeafNodeInt;

		// insert in sorted order
		sortedLeafEntry(currNode, pair, occupancy);

		// Now, copy half the keys from previous node to this one
		bool insertedNewEntry = false;
		int i = leafOccupancy / 2;
		while (i < leafOccupancy)
		{
			if (insertedNewEntry)
			{
				break;
			}
			if (pair.key <= currNode->keyArray[i])
			{ // if key < current key in node, insert it first
				newNode->keyArray[i] = pair.key;
				newNode->ridArray[i] = pair.rid;
				insertedNewEntry = true;
			}
			else
			{ // else keep inserting from currNode
				newNode->keyArray[i] = currNode->keyArray[i];
				newNode->ridArray[i] = currNode->ridArray[i];
			}
			i++;
		}
		// create new root which will be a Non leaf node
		NonLeafNodeInt *newInternalNode = new NonLeafNodeInt;
		newInternalNode->level = 1;

		// alloc new page for new non leaf node
		Page *newPage;
		PageId newPageId;
		bufMgr->allocPage(file, newPageId, newPage);

		// connect curr node to new leaf node
		currNode->rightSibPageNo = newPageId;
		int leftmostKey = newNode->keyArray[0];

		// copy up leftmost key on new node up to the root
		insertToNonLeaf(newInternalNode, newPageId, leftmostKey);
	}

	void BTreeIndex::splitNonLeaf(NonLeafNodeInt *currNode, PageId pageid, int key, int occupancy)
	{
		// create new non leafNode
		NonLeafNodeInt *newNode = new NonLeafNodeInt;

		// insert new key into current node in sorted order first
		sortedNonLeafEntry(currNode, key, occupancy);

		// copy half the keys from previous node to this one
		bool insertedNewEntry = false; // currently, not being used/checked
		int i = nodeOccupancy / 2;
		while (i < leafOccupancy)
		{
			if (insertedNewEntry)
			{
				break;
			}
			if (key <= currNode->keyArray[i])
			{
				newNode->keyArray[i] = key;
				insertedNewEntry = true;
			}
			else
			{
				newNode->keyArray[i] = currNode->keyArray[i];
				newNode->pageNoArray[i] = currNode->pageNoArray[i];
			}
			i++;
		}

		// create new root which will be a Non leaf node
		NonLeafNodeInt *newInternalNode = new NonLeafNodeInt;
		newInternalNode->level = currNode->level - 1;
		// alloc new page for new non leaf node
		Page *newPage;
		PageId newPageId;
		bufMgr->allocPage(file, newPageId, newPage);
		int leftmostKey = newNode->keyArray[0];

		// copy up leftmost key on new node up to the root
		insertToNonLeaf(newInternalNode, newPageId, leftmostKey);
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
		if (scanExecuting == true)
		{
			endScan();
		}
		// BadOpcodesException takes higher precedence over BadScanrangeException
		// only support GT and GTE here
		if (lowOpParm != GT || lowOpParm != GTE)
		{
			throw BadOpcodesException();
		}
		this->lowOp = lowOpParm;
		// only support LT and LTE here
		if (highOpParm != LT || highOpParm != LTE)
		{
			throw BadOpcodesException();
		}
		this->highOp = highOpParm;

		// store scan settings into instance
		if (this->attributeType == INTEGER)
		{
			this->lowValInt = *((int *)lowValParm);
			this->highValInt = *((int *)highValParm);

			// If lowValue > highValue, throw the exception BadScanrangeException.
			if (this->lowValInt > this->highValInt)
			{
				throw BadScanrangeException();
			}
		}

		else if (this->attributeType == DOUBLE)
		{
			this->lowValDouble = *((double *)lowValParm);
			this->highValDouble = *((double *)highValParm);

			// If lowValue > highValue, throw the exception BadScanrangeException.
			if (this->lowValDouble > this->highValDouble)
			{
				throw BadScanrangeException();
			}
		}
		else if (this->attributeType == STRING)
		{
			this->lowValString = (char *)lowValParm;
			this->highValString = (char *)highValParm;

			// If lowValue > highValue, throw the exception BadScanrangeException.
			if (this->lowValString.compare(this->highValString) > 0)
			{
				throw BadScanrangeException();
			}
		}

		// Both the high and low values are in a binary form, i.e., for integer
		// keys, these point to the address of an integer.

		// Start from root to find out the leaf page that contains the first RecordID
		// that satisfies the scan parameters. Keep that page pinned in the buffer pool.
		// currentPageNum = rootPageNum;
		// bufMgr->readPage(file, rootPageNum, currentPageData);
		// bufMgr->unPinPage(file, currentPageNum, true);
		scanExecuting = true;
		// Start from root to find out the leaf page that contains the first RecordID
		// that satisfies the scan parameters. Keep that page pinned in the buffer pool.
		bufMgr->readPage(file, rootPageNum, currentPageData);
		currentPageNum = rootPageNum;
		bufMgr->unPinPage(file, currentPageNum, false);
		// currentNode should start at the ROOT, which should be a NonLeafNode
		NonLeafNodeInt *currentNode = (NonLeafNodeInt *)currentPageData;

		// use the lowValParm to find the start of the range in the B-Tree
		// this works because you can only use GT or GTE with the lowValParm
		while (currentNode->level != 1)
		{
			int index = 0;
			while (true)
			{
				// index is past at or past the limit
				if (index >= nodeOccupancy)
				{
					break;
				}
				// check if page is valid
				//                       index or index+1
				if (currentNode->pageNoArray[index] == Page::INVALID_NUMBER)
				{
					break;
				}
				// check if the lowVal is less than the current key at the index
				// currrent only works with INTEGERS
				if (this->lowValInt < currentNode->keyArray[index])
				{
					break;
				}
				// increment and run again
				index++;
			}
			// Use the index we found to get the pageNo
			PageId nextNodePageNum = currentNode->pageNoArray[index];
			bufMgr->readPage(file, nextNodePageNum, currentPageData);
			bufMgr->unPinPage(file, nextNodePageNum, false);
			currentPageNum = nextNodePageNum;

			// go to next node
			currentNode = (NonLeafNodeInt *)currentPageData;
		}

		// TODO: (ANDY) still need to find LEAF and INDEX of starting position

		while (true)
		{
			int loop = 0;
			LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;
			for (int i = 0; i < leafOccupancy; i++)
			{
				int key = currentNode->keyArray[i];
				if ((this->lowOp == GTE && this->highOp == LTE) && (key >= this->lowOp && key <= this->highOp))
				{
					scanExecuting = true;
					loop = 1;
					nextEntry = i;
					break;
				}
				else if ((this->lowOp == GTE && this->highOp == LT) && (key >= this->lowOp && key < this->highOp))
				{
					scanExecuting = true;
					loop = 1;
					nextEntry = i;
					break;
				}
				else if ((this->lowOp == GT && this->highOp == LTE) && (key > this->lowOp && key <= this->highOp))
				{
					scanExecuting = true;
					loop = 1;
					nextEntry = i;
					break;
				}
				else if ((this->lowOp == GT && this->highOp == LT) && (key >= this->lowOp && key <= this->highOp))
				{
					scanExecuting = true;
					loop = 1;
					nextEntry = i;
					break;
				}
				else if ((this->lowOp == GTE && this->highOp == LT) && (key >= this->lowOp && key < this->highOp))
				{
					scanExecuting = true;
					loop = 1;
					break;
				}
				else if ((this->lowOp == GT && this->highOp == LTE) && (key > this->lowOp && key <= this->highOp))
				{
					scanExecuting = true;
					loop = 1;
					break;
				}
				else if ((this->lowOp == GT && this->highOp == LT) && (key >= this->lowOp && key <= this->highOp))
				{
					scanExecuting = true;
					loop = 1;
					break;
				}
				// Need to check which attributeType we are working with and then use that compare method properly
				// else if ((this->highOp == LT && key >= this->h) || (this->highOp == LTE && key > highValParm))
				// {
				// 	bufMgr->unPinPage(file, currentPageNum, false);
				// 	throw NoSuchKeyFoundException();
				// }
				// when i is the last one and still not out of loop so its not found in the node
				if (i == leafOccupancy - 1)
				{
					bufMgr->unPinPage(file, currentPageNum, false);
					if (currentNode->rightSibPageNo != 0)
					{
						currentPageNum = currentNode->rightSibPageNo;
						bufMgr->readPage(file, currentPageNum, currentPageData);
					}
					else
					{
						throw NoSuchKeyFoundException();
					}
				}
			}
			if (loop == 1)
			{
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
	//	 * Fetch the record id of the next index entry that matches the scan.
	//	 * Return the next record from current page being scanned. If current page has been scanned to its entirety,
	//   move on to the right sibling of current page, if any exists, to start scanning that page.
	//   Make sure to unpin any pages that are no longer required.
	//   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	// -----------------------------------------------------------------------------

	void BTreeIndex::scanNext(RecordId &outRid)
	{
		if (!scanExecuting)
		{
			throw ScanNotInitializedException();
		}
		bufMgr->readPage(file, currentPageNum, currentPageData);
		LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;

		if (nextEntry == leafOccupancy)
		{
			bufMgr->unPinPage(file, currentPageNum, false);
			if (currentNode->rightSibPageNo == 0)
			{
				throw IndexScanCompletedException();
			}
			else
			{
				currentPageNum = currentNode->rightSibPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
				currentNode = (LeafNodeInt *)currentPageData;
				nextEntry = 0;
			}
		}

		int key = currentNode->keyArray[nextEntry];
		if ((this->lowOp == GTE && this->highOp == LTE) && (key >= this->lowOp && key <= this->highOp))
		{
			outRid = key;
		}
		else if ((this->lowOp == GTE && this->highOp == LT) && (key >= this->lowOp && key < this->highOp))
		{
			outRid = key;
		}
		else if ((this->lowOp == GT && this->highOp == LTE) && (key > this->lowOp && key <= this->highOp))
		{
			outRid = key;
		}
		else if ((this->lowOp == GT && this->highOp == LT) && (key >= this->lowOp && key <= this->highOp))
		{
			outRid = key;
		}
		else
		{
			throw IndexScanCompletedException();
		}

		if (currentNode->rightSibPageNo == 0)
		{

			throw IndexScanCompletedException();
		}

		nextEntry++;
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
}
