package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete implements DbIterator {

    private static final long serialVersionUID = 1L;
    private boolean opened = false;
    private Tuple next = null;
    private int estimatedCardinality = 0;
    private final TransactionId transID;
    private DbIterator childDbIterator;
    private final TupleDesc td;
    private boolean fetchNext;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(final TransactionId t, final DbIterator child) {
	transID = t;
	childDbIterator = child;
	fetchNext = false;
	td = new TupleDesc(new Type[] { Type.INT_TYPE },
	        new String[] { "Deleted Record Counts" });
    }

    @Override
    public TupleDesc getTupleDesc() {
	return td;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
	opened = true;
	childDbIterator.open();
	fetchNext = true;
    }

    @Override
    public void close() {
	opened = false;
	childDbIterator.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
	childDbIterator.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	if (!opened)
	    throw new IllegalStateException("Operator not yet open");

	if (!fetchNext)
	    return null;
	else
	    fetchNext = false;

	int count = 0;
	while (childDbIterator.hasNext()) {
	    count++;
	    Tuple tuple = childDbIterator.next();
	    try {
		Database.getBufferPool().deleteTuple(transID, tuple);
	    }
	    catch (IOException e) {
		e.printStackTrace();
	    }
	}
	Tuple returnTuple = new Tuple(td);
	returnTuple.setField(0, new IntField(count));
	return returnTuple;
    }

    public DbIterator[] getChildren() {
	return new DbIterator[] { childDbIterator };
    }

    public void setChildren(final DbIterator[] children) {
	childDbIterator = children[0];
    }

    /*
     * (non-Javadoc)
     * @see simpledb.DbIterator#hasNext()
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
	if (!opened)
	    throw new IllegalStateException("Operator not yet open");
	if (next == null)
	    next = fetchNext();
	return next != null;
    }

    /*
     * (non-Javadoc)
     * @see simpledb.DbIterator#next()
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
	    NoSuchElementException {
	if (!opened)
	    throw new IllegalStateException("Operator not yet open");
	if (next == null) {
	    next = fetchNext();
	    if (next == null)
		throw new NoSuchElementException();
	}

	Tuple result = next;
	next = null;
	return result;
    }

    /**
     * @return The estimated cardinality of this operator. Will only be used in
     *         lab6
     * */
    public int getEstimatedCardinality() {
	return estimatedCardinality;
    }

    /**
     * @param card
     *            The estimated cardinality of this operator Will only be used
     *            in lab6
     * */
    protected void setEstimatedCardinality(final int card) {
	estimatedCardinality = card;
    }

}
