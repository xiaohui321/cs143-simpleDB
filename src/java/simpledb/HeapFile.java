package simpledb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private FileChannel fileChannel;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(final File f, final TupleDesc td) {
	file = f;
	tupleDesc = td;
   	RandomAccessFile raf = null;
	try {
		raf = new RandomAccessFile(f, "rw");
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	fileChannel = raf.getChannel();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
	return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
	return tupleDesc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(final PageId pid) {
	int pageNum = pid.pageNumber();
	int offset = pageNum * BufferPool.PAGE_SIZE;
	int length = (int) file.length();
	byte[] filebytes = new byte[length];
	byte[] pagebytes = null;
	try {
	    FileInputStream fileInputStream = new FileInputStream(file);
	    fileInputStream.read(filebytes);
	    pagebytes = Arrays.copyOfRange(filebytes, offset, offset
		    + BufferPool.PAGE_SIZE);
	    fileInputStream.close();
	    return new HeapPage((HeapPageId) pid, pagebytes);
	}
	catch (IOException e) {
	    e.printStackTrace();
	    return null;
	}
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(final Page page) throws IOException {
      	int pageNumber = page.getId().pageNumber();
    	int offset = pageNumber * BufferPool.PAGE_SIZE;
    	    	    	
    	try {
    		ByteBuffer buffer = ByteBuffer.wrap(page.getPageData());
    		fileChannel.write(buffer, offset);
    	} catch (IOException e) {
    		System.out.println("error writing page: " + e);
    		System.exit(1);
    	}
  
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
	return (int) Math.ceil(file.length() / BufferPool.PAGE_SIZE);
    }

    // allocate new pages to this file
    private HeapPage createNewPage() {
	// Initialize new page
	int tableId = getId();
	HeapPageId pid = new HeapPageId(tableId, numPages());
	byte[] PageContent = new byte[BufferPool.PAGE_SIZE];
	HeapPage newPage = null;
	try {
	    newPage = new HeapPage(pid, PageContent);
	    writePage(newPage);
	    return newPage;
	}
	catch (IOException e) {
	    e.printStackTrace();
	}

	return null;
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(final TransactionId tid, final Tuple t)
	    throws DbException, IOException, TransactionAbortedException {

	int tableId = getId();
	ArrayList<Page> modifiedPages = new ArrayList<Page>();

	// 1st: find empty HeapPages
	HeapPage emptyPage = null;
	for (int i = 0; i < numPages(); i++) {
	    emptyPage = (HeapPage) Database.getBufferPool().getPage(tid,
		    new HeapPageId(tableId, i), Permissions.READ_WRITE);
	    if (emptyPage.hasFreeSlots())
		break;
	}

	// Insert tuples
	// No empty page exists
	if (!emptyPage.hasFreeSlots()) {
	    HeapPage newPage = createNewPage();
	    newPage = (HeapPage) Database.getBufferPool().getPage(tid,
		    newPage.getId(), Permissions.READ_WRITE);
	    newPage.insertTuple(t);
	    newPage.markDirty(true, tid);
	    modifiedPages.add(newPage);
	} else {  // Empty page exists
	    HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid,
		    emptyPage.getId(), Permissions.READ_WRITE);
	    newPage.insertTuple(t);
	    newPage.markDirty(true, tid);
	    modifiedPages.add(newPage);
	}
	return modifiedPages;
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(final TransactionId tid, final Tuple t)
	    throws DbException, TransactionAbortedException {
	ArrayList<Page> modifiedPages = new ArrayList<Page>();
	HeapPage editPage = (HeapPage) Database.getBufferPool().getPage(tid,
	        t.getRecordId().getPageId(), Permissions.READ_WRITE);
	editPage.deleteTuple(t);
	editPage.markDirty(true, tid);
	modifiedPages.add(editPage);
	return modifiedPages;
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(final TransactionId tid) {
	return new HeapFileIterator(tid, this);
    }

}
