package simpledb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * OrderBy is an operator that implements a relational ORDER BY.
 */
public class OrderBy implements DbIterator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private final TupleDesc td;
    private final ArrayList<Tuple> childTups = new ArrayList<Tuple>();
    private final int orderByField;
    private final String orderByFieldName;
    private Iterator<Tuple> it;
    private final boolean asc;
    private boolean opened = false;
    private Tuple next = null;
    private int estimatedCardinality = 0;

    /**
     * Creates a new OrderBy node over the tuples from the iterator.
     * 
     * @param orderbyField
     *            the field to which the sort is applied.
     * @param asc
     *            true if the sort order is ascending.
     * @param child
     *            the tuples to sort.
     */
    public OrderBy(final int orderbyField, final boolean asc,
	    final DbIterator child) {
	this.child = child;
	td = child.getTupleDesc();
	orderByField = orderbyField;
	orderByFieldName = td.getFieldName(orderbyField);
	this.asc = asc;
    }

    public boolean isASC() {
	return asc;
    }

    public int getOrderByField() {
	return orderByField;
    }

    public String getOrderFieldName() {
	return orderByFieldName;
    }

    @Override
    public TupleDesc getTupleDesc() {
	return td;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
	    TransactionAbortedException {
	child.open();
	// load all the tuples in a collection, and sort it
	while (child.hasNext())
	    childTups.add(child.next());
	Collections.sort(childTups, new TupleComparator(orderByField, asc));
	it = childTups.iterator();
	opened = true;
    }

    @Override
    public void close() {
	opened = false;
	it = null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
	it = childTups.iterator();
    }

    /**
     * Operator.fetchNext implementation. Returns tuples from the child operator
     * in order
     * 
     * @return The next tuple in the ordering, or null if there are no more
     *         tuples
     */
    protected Tuple fetchNext() throws NoSuchElementException,
	    TransactionAbortedException, DbException {
	if (it != null && it.hasNext())
	    return it.next();
	else
	    return null;
    }

    public DbIterator[] getChildren() {
	return new DbIterator[] { child };
    }

    public void setChildren(final DbIterator[] children) {
	child = children[0];
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

class TupleComparator implements Comparator<Tuple> {
    int field;
    boolean asc;

    public TupleComparator(final int field, final boolean asc) {
	this.field = field;
	this.asc = asc;
    }

    @Override
    public int compare(final Tuple o1, final Tuple o2) {
	Field t1 = o1.getField(field);
	Field t2 = o2.getField(field);
	if (t1.compare(Predicate.Op.EQUALS, t2))
	    return 0;
	if (t1.compare(Predicate.Op.GREATER_THAN, t2))
	    return asc ? 1 : -1;
	else
	    return asc ? -1 : 1;
    }

}
