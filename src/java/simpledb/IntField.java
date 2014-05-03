package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Instance of Field that stores a single integer.
 */
public class IntField implements Field {

    private static final long serialVersionUID = 1L;

    private final int value;

    public int getValue() {
	return value;
    }

    /**
     * Constructor.
     * 
     * @param i
     *            The value of this field.
     */
    public IntField(final int i) {
	value = i;
    }

    @Override
    public String toString() {
	return Integer.toString(value);
    }

    @Override
    public int hashCode() {
	return value;
    }

    @Override
    public boolean equals(final Object field) {
	return ((IntField) field).value == value;
    }

    @Override
    public void serialize(final DataOutputStream dos) throws IOException {
	dos.writeInt(value);
    }

    /**
     * Compare the specified field to the value of this Field. Return semantics
     * are as specified by Field.compare
     * 
     * @throws IllegalCastException
     *             if val is not an IntField
     * @see Field#compare
     */
    @Override
    public boolean compare(final Predicate.Op op, final Field val) {

	IntField iVal = (IntField) val;

	switch (op) {
	    case EQUALS:
		return value == iVal.value;
	    case NOT_EQUALS:
		return value != iVal.value;
	    case GREATER_THAN:
		return value > iVal.value;
	    case GREATER_THAN_OR_EQ:
		return value >= iVal.value;
	    case LESS_THAN:
		return value < iVal.value;
	    case LESS_THAN_OR_EQ:
		return value <= iVal.value;
	    case LIKE:
		return value == iVal.value;
	    default:
		return false;
	}
    }

    /**
     * Return the Type of this field.
     * 
     * @return Type.INT_TYPE
     */
    @Override
    public Type getType() {
	return Type.INT_TYPE;
    }
}
