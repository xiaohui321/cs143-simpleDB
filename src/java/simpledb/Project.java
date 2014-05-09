package simpledb;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Project is an operator that implements a relational projection.
 */
public class Project implements DbIterator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private final TupleDesc td;
    private final ArrayList<Integer> outFieldIds;
    private boolean opened = false;
    private Tuple next = null;
    private int estimatedCardinality = 0;

    /**
     * Constructor accepts a child operator to read tuples to apply projection
     * to and a list of fields in output tuple
     * 
     * @param fieldList
     *            The ids of the fields child's tupleDesc to project out
     * @param typesList
     *            the types of the fields in the final projection
     * @param child
     *            The child operator
     */
    public Project(final ArrayList<Integer> fieldList,
	    final ArrayList<Type> typesList, final DbIterator child) {
	this(fieldList, typesList.toArray(new Type[] {}), child);
    }

    public Project(final ArrayList<Integer> fieldList, final Type[] types,
	    final DbIterator child) {
	this.child = child;
	outFieldIds = fieldList;
	String[] fieldAr = new String[fieldList.size()];
	TupleDesc childtd = child.getTupleDesc();

	for (int i = 0; i < fieldAr.length; i++)
	    fieldAr[i] = childtd.getFieldName(fieldList.get(i));
	td = new TupleDesc(types, fieldAr);
    }

    @Override
    public TupleDesc getTupleDesc() {
	return td;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
	    TransactionAbortedException {
	opened = true;
	child.open();
    }

    @Override
    public void close() {
	child.close();
	opened = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
	child.rewind();
    }

    /**
     * Operator.fetchNext implementation. Iterates over tuples from the child
     * operator, projecting out the fields from the tuple
     * 
     * @return The next tuple, or null if there are no more tuples
     */
    protected Tuple fetchNext() throws NoSuchElementException,
	    TransactionAbortedException, DbException {
	while (child.hasNext()) {
	    Tuple t = child.next();
	    Tuple newTuple = new Tuple(td);
	    newTuple.setRecordId(t.getRecordId());
	    for (int i = 0; i < td.numFields(); i++)
		newTuple.setField(i, t.getField(outFieldIds.get(i)));
	    return newTuple;
	}
	return null;
    }

    public DbIterator[] getChildren() {
	return new DbIterator[] { child };
    }

    public void setChildren(final DbIterator[] children) {
	if (child != children[0])
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
