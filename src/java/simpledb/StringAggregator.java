package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByFieldInt, aggregateFieldInt;
    private String aggregateFieldName, groupByFieldName;
    private final Type groupByFieldType;
    private final Map<Field, Field> aggregateMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException
     *             if what != COUNT
     */
    public StringAggregator(final int gbfield, final Type gbfieldtype,
	    final int afield, final Op what) {
	if (!what.equals(Op.COUNT))
	    throw new IllegalArgumentException("The Operator must be COUNT");

	aggregateFieldInt = afield;
	aggregateFieldName = "";
	aggregateMap = new HashMap<Field, Field>();

	groupByFieldInt = gbfield;
	groupByFieldName = "";
	groupByFieldType = gbfieldtype;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(final Tuple tup) {
	Field keyField;
	if (groupByFieldInt == NO_GROUPING)
	    keyField = new IntField(NO_GROUPING);
	else {
	    keyField = tup.getField(groupByFieldInt);
	    groupByFieldName = tup.getTupleDesc().getFieldName(groupByFieldInt);
	}
	aggregateFieldName = tup.getTupleDesc().getFieldName(aggregateFieldInt);
	IntField valueField = (IntField) aggregateMap.get(keyField);

	int newValue = 1;
	if (valueField != null)
	    newValue += valueField.getValue();

	IntField newValueField = new IntField(newValue);
	aggregateMap.put(keyField, newValueField);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    @Override
    public DbIterator iterator() {
	if (groupByFieldInt == NO_GROUPING) {
	    Type[] types = new Type[] { Type.INT_TYPE };
	    String[] strings = new String[] { aggregateFieldName };
	    TupleDesc tupleDesc = new TupleDesc(types, strings);
	    Tuple tuple = new Tuple(tupleDesc);
	    tuple.setField(0, aggregateMap.get(new IntField(NO_GROUPING)));
	    List<Tuple> tupleList = new ArrayList<Tuple>();
	    tupleList.add(tuple);
	    return new TupleIterator(tupleDesc, tupleList);
	} else {
	    Type[] types = new Type[] { groupByFieldType, Type.INT_TYPE };
	    String[] strings = new String[] { groupByFieldName,
		    aggregateFieldName };
	    TupleDesc tupleDesc = new TupleDesc(types, strings);
	    List<Tuple> tupleList = new ArrayList<Tuple>();
	    Set<Field> keySet = aggregateMap.keySet();
	    for (Field i : keySet) {
		Tuple tuple = new Tuple(tupleDesc);
		tuple.setField(0, i);
		tuple.setField(1, aggregateMap.get(i));
		tupleList.add(tuple);
	    }
	    return new TupleIterator(tupleDesc, tupleList);
	}
    }

}
