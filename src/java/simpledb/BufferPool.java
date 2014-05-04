package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final Map<PageId, Page> pageMap;
    private final int maxNumPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     * 
     * @param numPages
     *            maximum number of pages in this buffer pool.
     */
    public BufferPool(final int numPages) {
	maxNumPages = numPages;
	pageMap = new HashMap<PageId, Page>();
    }

    public static int getPageSize() {
	return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(final int pageSize) {
	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire
     * a lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is
     * present, it should be returned. If it is not present, it should be added
     * to the buffer pool and returned. If there is insufficient space in the
     * buffer pool, an page should be evicted and the new page should be added
     * in its place.
     * 
     * @param tid
     *            the ID of the transaction requesting the page
     * @param pid
     *            the ID of the requested page
     * @param perm
     *            the requested permissions on the page
     */
    public Page getPage(final TransactionId tid, final PageId pid,
	    final Permissions perm) throws TransactionAbortedException,
	    DbException {
	// TODO: add lock
	if (pageMap.containsKey(pid))
	    return pageMap.get(pid);

	// page not found in buffer
	Page newPage = Database.getCatalog().getPage(pid);

	if (pageMap.size() == maxNumPages)
	    evictPage();

	pageMap.put(pid, newPage);
	return newPage;
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result
     * in wrong behavior. Think hard about who needs to call this and why, and
     * why they can run the risk of calling it.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param pid
     *            the ID of the page to unlock
     */
    public void releasePage(final TransactionId tid, final PageId pid) {
	// some code goes here
	// not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     */
    public void transactionComplete(final TransactionId tid) throws IOException {
	// some code goes here
	// not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(final TransactionId tid, final PageId p) {
	// some code goes here
	// not necessary for lab1|lab2
	return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param commit
     *            a flag indicating whether we should commit or abort
     */
    public void transactionComplete(final TransactionId tid,
	    final boolean commit) throws IOException {
	// some code goes here
	// not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2). May
     * block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     * 
     * @param tid
     *            the transaction adding the tuple
     * @param tableId
     *            the table to add the tuple to
     * @param t
     *            the tuple to add
     */
    public void insertTuple(final TransactionId tid, final int tableId,
	    final Tuple t) throws DbException, IOException,
	    TransactionAbortedException {
	// TODO:unfinished
	Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write
     * lock on the page the tuple is removed from and any other pages that are
     * updated. May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     * 
     * @param tid
     *            the transaction deleting the tuple.
     * @param t
     *            the tuple to delete
     */
    public void deleteTuple(final TransactionId tid, final Tuple t)
	    throws DbException, IOException, TransactionAbortedException {
	// some code goes here
	// not necessary for lab1
	// TODO:unfinished
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it
     * writes dirty data to disk so will break simpledb if running in NO STEAL
     * mode.
     */
    public synchronized void flushAllPages() throws IOException {
	for (PageId pid : pageMap.keySet())
	    flushPage(pid);
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in
     * its cache.
     */
    public synchronized void discardPage(final PageId pid) {
	// some code goes here
	// only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid
     *            an ID indicating the page to flush
     */
    private synchronized void flushPage(final PageId pid) throws IOException {
	// some code goes here
	// not necessary for lab1
	// TODO:unfinished
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(final TransactionId tid)
	    throws IOException {
	// some code goes here
	// not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
	// some code goes here
	// not necessary for lab1
	// TODO:unfinished
    }

}
