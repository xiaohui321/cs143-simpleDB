package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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
     *            the aggregation operator
     */

    private final int groupByFieldInt, aggregateFieldInt;
    private String aggregateFieldName, groupByFieldName;
    private final Type groupByFieldType;
    private final Op operator;
    private final Map<Field, Field> aggregateMap;
    private final Map<Field, Average> averageMap;

    private class Average {
	private int sum, count;

	public Average() {
	    sum = 0;
	    count = 0;
	}

	public int getAverage() {
	    return sum / count;
	}

	public void addNewItem(final int value) {
	    sum += value;
	    count++;
	}
    }

    public IntegerAggregator(final int gbfield, final Type gbfieldtype,
	    final int afield, final Op what) {
	aggregateFieldInt = afield;
	aggregateFieldName = "";
	aggregateMap = new HashMap<Field, Field>();

	groupByFieldInt = gbfield;
	groupByFieldName = "";
	groupByFieldType = gbfieldtype;

	operator = what;
	averageMap = new HashMap<Field, Average>();
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
	aggregateFieldName = tup.getTupleDesc().getFieldName(aggregateFieldInt);

	// create the key field based on type
	if (groupByFieldInt == NO_GROUPING)
	    keyField = new IntField(NO_GROUPING);
	else {
	    keyField = tup.getField(groupByFieldInt);
	    groupByFieldName = tup.getTupleDesc().getFieldName(groupByFieldInt);
	}
	IntField valueField = (IntField) aggregateMap.get(keyField);
	IntField tupleCorrespondingValueField = (IntField) tup
	        .getField(aggregateFieldInt);
	int newValue = 0;

	// do grouping based on the operator
	switch (operator) {
	    case AVG:
		if (valueField == null) {
		    // the initial setup for getting average.
		    Average newAverage = new Average();
		    newValue = tupleCorrespondingValueField.getValue();
		    newAverage.addNewItem(newValue);
		    averageMap.put(keyField, newAverage);
		} else {
		    Average oldAverage = averageMap.get(keyField);
		    oldAverage.addNewItem(tupleCorrespondingValueField
			    .getValue());
		    averageMap.put(keyField, oldAverage);
		    newValue = oldAverage.getAverage();
		}
		break;
	    case COUNT:
		newValue = 1;
		if (valueField != null)
		    newValue += valueField.getValue();
		break;
	    case MAX:
		newValue = tupleCorrespondingValueField.getValue();
		if (valueField != null && valueField.getValue() > newValue)
		    newValue = valueField.getValue();
		break;
	    case MIN:
		newValue = tupleCorrespondingValueField.getValue();
		if (valueField != null && valueField.getValue() < newValue)
		    newValue = valueField.getValue();
		break;
	    case SC_AVG:
		throw new UnsupportedOperationException("unimplemented");
	    case SUM:
		newValue = tupleCorrespondingValueField.getValue();
		if (valueField != null)
		    newValue += valueField.getValue();
		break;
	    case SUM_COUNT:
		throw new UnsupportedOperationException("unimplemented");
	    default:
		// we should never get there
		throw new UnsupportedOperationException("Unknown operator");

	}
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
	// create iterator based on grouping status
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
