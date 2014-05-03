package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param operator
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    private final int fieldNum1, fieldNum2;
    private final Predicate.Op operator;

    public JoinPredicate(final int field1, final Predicate.Op op,
	    final int field2) {
	fieldNum1 = field1;
	fieldNum2 = field2;
	operator = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(final Tuple t1, final Tuple t2) {
	return t1.getField(fieldNum1).compare(operator, t2.getField(fieldNum2));
    }

    public int getField1() {
	return fieldNum1;
    }

    public int getField2() {
	return fieldNum2;
    }

    public Predicate.Op getOperator() {
	return operator;
    }
}
