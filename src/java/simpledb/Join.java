package simpledb;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join implements DbIterator {

    private static final long serialVersionUID = 1L;

    private Tuple next = null;
    private int estimatedCardinality = 0;
    private final JoinPredicate joinPredicate;
    private DbIterator childDbIterator1, childDbIterator2;
    private Tuple childTuple1;
    private boolean child1ShouldGetNext;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(final JoinPredicate p, final DbIterator child1,
	    final DbIterator child2) {
	joinPredicate = p;
	childDbIterator1 = child1;
	childDbIterator2 = child2;
	childTuple1 = null;
	child1ShouldGetNext = true;

    }

    public JoinPredicate getJoinPredicate() {
	return joinPredicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by alias or
     *         table name.
     * */
    public String getJoinField1Name() {
	return childDbIterator1.getTupleDesc().getFieldName(
	        joinPredicate.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by alias or
     *         table name.
     * */
    public String getJoinField2Name() {
	return childDbIterator2.getTupleDesc().getFieldName(
	        joinPredicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */

    @Override
    public TupleDesc getTupleDesc() {
	TupleDesc tupleDesc1 = childDbIterator1.getTupleDesc();
	TupleDesc tupleDesc2 = childDbIterator2.getTupleDesc();
	return TupleDesc.merge(tupleDesc1, tupleDesc2);
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
	    TransactionAbortedException {
	childDbIterator1.open();
	childDbIterator2.open();

    }

    @Override
    public void close() {
	childDbIterator1.close();
	childDbIterator2.close();

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
	childDbIterator1.rewind();
	childDbIterator2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	do {
	    if (child1ShouldGetNext)
		if (childDbIterator1.hasNext())
		    childTuple1 = childDbIterator1.next();
		else {
		    childTuple1 = null;
		    return null;
		}
	    while (childDbIterator2.hasNext()) {
		Tuple childTuple2 = childDbIterator2.next();
		if (joinPredicate.filter(childTuple1, childTuple2)) {
		    Tuple newTuple = new Tuple(getTupleDesc());
		    int i = 0, size_child1 = childTuple1.getTupleDesc()
			    .numFields();
		    for (; i < size_child1; i++)
			newTuple.setField(i, childTuple1.getField(i));
		    for (; i < newTuple.getTupleDesc().numFields(); i++)
			newTuple.setField(i,
			        childTuple2.getField(i - size_child1));
		    child1ShouldGetNext = false;
		    return newTuple;
		}
	    }
	    childDbIterator2.rewind();
	    child1ShouldGetNext = true;
	}
	while (childTuple1 != null);
	return null;
    }

    public DbIterator[] getChildren() {
	return new DbIterator[] { childDbIterator1, childDbIterator2 };
    }

    public void setChildren(final DbIterator[] children) {
	childDbIterator1 = children[0];
	childDbIterator2 = children[1];
    }

    /*
     * (non-Javadoc)
     * @see simpledb.DbIterator#hasNext()
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
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
