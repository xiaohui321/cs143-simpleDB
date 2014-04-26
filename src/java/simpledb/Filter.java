package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */

    private final Predicate predicate;
    private DbIterator childDbIterator;

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
	childDbIterator.open();
	childDbIterator.rewind();
    }

    @Override
    public void close() {
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
    @Override
    protected Tuple fetchNext() throws NoSuchElementException,
	    TransactionAbortedException, DbException {
	while (childDbIterator.hasNext()) {
	    Tuple tuple = childDbIterator.next();
	    if (predicate.filter(tuple))
		return tuple;
	}
	return null;
    }

    @Override
    public DbIterator[] getChildren() {
	return new DbIterator[] { childDbIterator };
    }

    @Override
    public void setChildren(final DbIterator[] children) {
	childDbIterator = children[0];
    }

}
