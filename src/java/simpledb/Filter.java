package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter implements DbIterator {

    private static final long serialVersionUID = 1L;
    private boolean opened = false;
    private Tuple next = null;
    private int estimatedCardinality = 0;
    private final Predicate predicate;
    private DbIterator childDbIterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(final Predicate p, final DbIterator child) {
	predicate = p;
	childDbIterator = child;
    }

    public Predicate getPredicate() {
	return predicate;
    }

    @Override
    public TupleDesc getTupleDesc() {
	return childDbIterator.getTupleDesc();
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
	    TransactionAbortedException {
	opened = true;
	childDbIterator.open();
    }

    @Override
    public void close() {
	next = null;
	opened = false;
	childDbIterator.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
	childDbIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
	    TransactionAbortedException, DbException {
	if (!opened)
	    throw new IllegalStateException("Operator not yet open");
	while (childDbIterator.hasNext()) {
	    Tuple tuple = childDbIterator.next();
	    if (predicate.filter(tuple))
		return tuple;
	}
	return null;
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
