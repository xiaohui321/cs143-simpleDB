package simpledb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

	File file;
	TupleDesc tupleDesc;

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
      int offset = pageNum*BufferPool.PAGE_SIZE;
      int length = (int) file.length();
      byte[] filebytes = new byte[length];
      byte[] pagebytes = null;
      try{
    	  FileInputStream fileInputStream = new FileInputStream(file);
          fileInputStream.read(filebytes);
          pagebytes = Arrays.copyOfRange(filebytes, offset, offset+BufferPool.PAGE_SIZE);    	  
      }catch(IOException e){
    	  e.printStackTrace();
      }
      
	  try {
			return new HeapPage((HeapPageId)pid, pagebytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;	
		}
	}

	// see DbFile.java for javadocs
	@Override
	public void writePage(final Page page) throws IOException {
		// TODO: some code goes here
		// not necessary for lab1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) Math.ceil(file.length()/BufferPool.PAGE_SIZE);
	}

	// see DbFile.java for javadocs
	@Override
	public ArrayList<Page> insertTuple(final TransactionId tid, final Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// TODO: some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	@Override
	public ArrayList<Page> deleteTuple(final TransactionId tid, final Tuple t)
			throws DbException, TransactionAbortedException {
		// TODO: some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	@Override
	public DbFileIterator iterator(final TransactionId tid) {
		return new HeapFileIterator(tid, this);
	}

}
