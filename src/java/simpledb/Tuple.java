package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tuple_desc;

    private RecordId id;

    private Field[] tuple_values;

    private int size_tuple;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(final TupleDesc td) {
	tuple_desc = td;
	id = null;
	size_tuple = tuple_desc.numFields();
	tuple_values = new Field[size_tuple];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
	return tuple_desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
	return id;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(final RecordId rid) {
	id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(final int i, final Field f)
	    throws NoSuchElementException {
	if (i < 0 || i >= size_tuple)
	    throw new NoSuchElementException("Invalid index");

	tuple_values[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(final int i) {
	if (i < 0 || i >= size_tuple)
	    throw new NoSuchElementException("Invalid index");

	return tuple_values[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    @Override
    public String toString() {
	String result = "";
	for (int i = 0; i < size_tuple - 1; i++)
	    result += tuple_values[i] + "\t";
	result += tuple_values[size_tuple - 1] + "\n";
	return result;
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
	return Arrays.asList(tuple_values).iterator();
    }

    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(final TupleDesc td) {
	tuple_desc = td;
	size_tuple = tuple_desc.numFields();
	tuple_values = new Field[size_tuple];
    }
}
