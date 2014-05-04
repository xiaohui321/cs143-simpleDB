package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert implements DbIterator {

    private static final long serialVersionUID = 1L;
    private boolean opened = false;
    private Tuple next = null;
    private int estimatedCardinality = 0;

    private final TransactionId transID;
    private DbIterator childDbIterator;
    private final int tableid;
    private final TupleDesc td;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(final TransactionId t, final DbIterator child,
	    final int tableid) throws DbException {
	transID = t;
	childDbIterator = child;
	this.tableid = tableid;
	td = new TupleDesc(new Type[] { Type.STRING_TYPE },
	        new String[] { "Inserted Record Counts" });
    }

    @Override
    public TupleDesc getTupleDesc() {
	return td;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
	opened = true;
	childDbIterator.open();
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	if (!opened)
	    throw new IllegalStateException("Operator not yet open");
	int count = 0;
	while (childDbIterator.hasNext()) {
	    count++;
	    Tuple tuple = childDbIterator.next();
	    try {
		Database.getBufferPool().insertTuple(transID, tableid, tuple);
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
