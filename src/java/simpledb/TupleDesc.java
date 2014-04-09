package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * A help class to facilitate organizing the information of each field
	 * */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 * */
		public final Type fieldType;

		/**
		 * The name of the field
		 * */
		public final String fieldName;

		public TDItem(final Type t, final String n) {
			fieldName = n;
			fieldType = t;
		}

		@Override
		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	private final List<TDItem> tupleDescList;

	/**
	 * @return An iterator which iterates over all the field TDItems that are
	 *         included in this TupleDesc
	 * */
	public Iterator<TDItem> iterator() {
		Iterator<TDItem> it = tupleDescList.iterator();
		return it;
	}

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 * 
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 * @param fieldAr
	 *            array specifying the names of the fields. Note that names may
	 *            be null.
	 */
	public TupleDesc(final Type[] typeAr, final String[] fieldAr) {
		tupleDescList = new ArrayList<TDItem>();
		for (int i = 0; i < typeAr.length; i++)
			tupleDescList.add(new TDItem(typeAr[i], fieldAr[i]));
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 * 
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(final Type[] typeAr) {
		tupleDescList = new ArrayList<TDItem>();
		for (Type element : typeAr)
			tupleDescList.add(new TDItem(element, null));
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		return tupleDescList.size();
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 * 
	 * @param i
	 *            index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public String getFieldName(final int i) throws NoSuchElementException {
		if (i < 0 || i >= tupleDescList.size())
			throw new NoSuchElementException("Invalid index");
		else
			return tupleDescList.get(i).fieldName;
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 * 
	 * @param i
	 *            The index of the field to get the type of. It must be a valid
	 *            index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public Type getFieldType(final int i) throws NoSuchElementException {
		if (i < 0 || i >= tupleDescList.size())
			throw new NoSuchElementException("Invalid index");
		else
			return tupleDescList.get(i).fieldType;
	}

	/**
	 * Find the index of the field with a given name.
	 * 
	 * @param name
	 *            name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException
	 *             if no field with a matching name is found.
	 */
	public int fieldNameToIndex(final String name)
			throws NoSuchElementException {

		for (TDItem element : tupleDescList)
			if (element.fieldName != null && element.fieldName.equals(name))
				return tupleDescList.indexOf(element);

		throw new NoSuchElementException("no field found with that name");
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		int totalSize = 0;
		for (TDItem element : tupleDescList)
			totalSize += element.fieldType.getLen();
		return totalSize;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 * 
	 * @param td1
	 *            The TupleDesc with the first fields of the new TupleDesc
	 * @param td2
	 *            The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(final TupleDesc td1, final TupleDesc td2) {
		Type[] typeArray = new Type[td1.numFields() + td2.numFields()];
		String[] fieldArray = new String[td1.numFields() + td2.numFields()];

		int i = 0;
		for (; i < td1.numFields(); i++) {
			typeArray[i] = td1.getFieldType(i);
			fieldArray[i] = td1.getFieldName(i);
		}

		for (int j = 0; j < td2.numFields(); j++) {
			typeArray[i] = td2.getFieldType(j);
			fieldArray[i] = td2.getFieldName(j);
			i++;
		}

		return new TupleDesc(typeArray, fieldArray);
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 * 
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	@Override
	public boolean equals(final Object o) {
		/* check argument type */
		if (!(o instanceof TupleDesc))
			return false;

		TupleDesc obj = (TupleDesc) o;

		/* check number of fields */
		if (obj.numFields() != numFields())
			return false;

		/* check each pair of fieldTypes */
		for (int i = 0; i < numFields(); i++)
			if (!obj.getFieldType(i).equals(getFieldType(i)))
				return false;

		return true;
	}

	@Override
	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	@Override
	public String toString() {
		String result = "";
		for (int i = 0; i < numFields(); i++)
			result += tupleDescList.get(i).fieldType + "[" + i + "]" + "("
					+ tupleDescList.get(i).fieldName + "[" + i + "]), ";
		return result.substring(0, result.length() - 2);
	}
}
