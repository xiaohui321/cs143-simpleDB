package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate implements DbIterator {

    private static final long serialVersionUID = 1L;
    private boolean opened = false;
    private DbIterator childDbIterator;
    private DbIterator aggregateIterator;
    private final int aggregateFieldInt, groupByFieldInt;
    private final Aggregator.Op operator;
    private Aggregator aggregator;
    private Tuple next = null;
    private int estimatedCardinality = 0;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(final DbIterator child, final int afield,
	    final int gfield, final Aggregator.Op aop) {
	childDbIterator = child;
	aggregateIterator = null;
	aggregateFieldInt = afield;
	groupByFieldInt = gfield;
	operator = aop;
	Type groupByfieldType = groupByFieldInt == Aggregator.NO_GROUPING ? null
	        : childDbIterator.getTupleDesc().getFieldType(groupByFieldInt);

	// create aggregator based on type
	if (childDbIterator.getTupleDesc().getFieldType(aggregateFieldInt) == Type.STRING_TYPE)
	    aggregator = new StringAggregator(groupByFieldInt,
		    groupByfieldType, aggregateFieldInt, operator);
	else
	    aggregator = new IntegerAggregator(groupByFieldInt,
		    groupByfieldType, aggregateFieldInt, operator);

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	return groupByFieldInt;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	if (groupByFieldInt == Aggregator.NO_GROUPING)
	    return null;
	else
	    return childDbIterator.getTupleDesc().getFieldName(groupByFieldInt);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	return aggregateFieldInt;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	return childDbIterator.getTupleDesc().getFieldName(aggregateFieldInt);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	return operator;
    }

    public static String nameOfAggregatorOp(final Aggregator.Op aop) {
	return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	opened = true;
	childDbIterator.open();
	while (childDbIterator.hasNext())
	    aggregator.mergeTupleIntoGroup(childDbIterator.next());
	aggregateIterator = aggregator.iterator();
	aggregateIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// check open satatus
	if (!opened)
	    throw new IllegalStateException("Operator not yet open");

	// sanity check
	if (aggregateIterator == null)
	    throw new DbException("open() not called yet");

	if (aggregateIterator.hasNext())
	    return aggregateIterator.next();
	else
	    return null;

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
	childDbIterator.rewind();
	aggregateIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
	if (aggregateFieldInt == Aggregator.NO_GROUPING) {
	    Type[] type = new Type[] { childDbIterator.getTupleDesc()
		    .getFieldType(aggregateFieldInt) };

	    String[] name = new String[] { operator.toString()
		    + "("
		    + childDbIterator.getTupleDesc().getFieldName(
		            aggregateFieldInt) + ")" };

	    return new TupleDesc(type, name);
	} else {
	    Type[] type = new Type[] {
		    childDbIterator.getTupleDesc()
		            .getFieldType(groupByFieldInt),
		    childDbIterator.getTupleDesc().getFieldType(
		            aggregateFieldInt) };

	    String[] name = new String[] {
		    childDbIterator.getTupleDesc()
		            .getFieldName(groupByFieldInt),
		    operator.toString()
		            + "("
		            + childDbIterator.getTupleDesc().getFieldName(
		                    aggregateFieldInt) + ")" };
	    return new TupleDesc(type, name);
	}

    }

    @Override
    public void close() {
	opened = false;
	childDbIterator.close();
	aggregateIterator.close();
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
	// check open status
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
