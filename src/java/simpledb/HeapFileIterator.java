package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    private final TransactionId tid;
    private final HeapFile file;
    private Iterator<Tuple> tupleiterator;
    private int currentpagenum;
    private HeapPage currentpage;
    private final int numpages;

    public HeapFileIterator(final TransactionId tid_, final HeapFile file_) {
	tid = tid_;
	file = file_;
	currentpagenum = 0;
	numpages = file.numPages();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
	currentpage = readpages(currentpagenum++);
	tupleiterator = currentpage.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
	if (tupleiterator == null)
	    return false;
	if (tupleiterator.hasNext())
	    return true;

	// if we reach here, this means current page runs out of tuples. Update
	// the current page
	while (currentpagenum <= numpages - 1) {
	    currentpage = readpages(currentpagenum++);
	    tupleiterator = currentpage.iterator();
	    if (tupleiterator.hasNext())
		return true;
	}
	return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
	    NoSuchElementException {
	if (tupleiterator == null)
	    throw new NoSuchElementException("Iterator not exit");
	return tupleiterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
	close();
	open();
    }

    @Override
    public void close() {
	currentpagenum = 0;
	tupleiterator = null;
    }

    private HeapPage readpages(final int pageNum)
	    throws TransactionAbortedException, DbException {
	HeapPageId page_id = new HeapPageId(file.getId(), pageNum);
	return (HeapPage) Database.getBufferPool().getPage(tid, page_id,
	        Permissions.READ_ONLY);
    }
}
