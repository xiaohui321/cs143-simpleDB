package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(final String tablename) {
	return statsMap.get(tablename);
    }

    public static void setTableStats(final String tablename,
	    final TableStats stats) {
	statsMap.put(tablename, stats);
    }

    public static void setStatsMap(final HashMap<String, TableStats> s) {
	try {
	    java.lang.reflect.Field statsMapF = TableStats.class
		    .getDeclaredField("statsMap");
	    statsMapF.setAccessible(true);
	    statsMapF.set(null, s);
	}
	catch (NoSuchFieldException e) {
	    e.printStackTrace();
	}
	catch (SecurityException e) {
	    e.printStackTrace();
	}
	catch (IllegalArgumentException e) {
	    e.printStackTrace();
	}
	catch (IllegalAccessException e) {
	    e.printStackTrace();
	}

    }

    public static Map<String, TableStats> getStatsMap() {
	return statsMap;
    }

    public static void computeStatistics() {
	Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

	System.out.println("Computing table stats.");
	while (tableIt.hasNext()) {
	    int tableid = tableIt.next();
	    TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
	    setTableStats(Database.getCatalog().getTableName(tableid), s);
	}
	System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final DbFile databaseFile;

    private final TupleDesc td;

    private final int ioCostPerPage;

    private final int tableid;

    private int num_tuples;

    private final Map<String, IntHistogram> intHistograms;

    private final Map<String, StringHistogram> stringHistograms;

    private class MinMax {
	public int max, min;
    }

    private final Map<String, MinMax> minMaxMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(final int tableid, final int ioCostPerPage) {
	// For this function, you'll have to get the
	// DbFile for the table in question,
	// then scan through its tuples and calculate
	// the values that you need.
	// You should try to do this reasonably efficiently, but you don't
	// necessarily have to (for example) do everything
	// in a single scan of the table.
	// TODO: some code goes here
	databaseFile = simpledb.Database.getCatalog().getDatabaseFile(tableid);
	this.tableid = tableid;
	this.ioCostPerPage = ioCostPerPage;
	num_tuples = 0;
	td = databaseFile.getTupleDesc();

	intHistograms = new HashMap<String, IntHistogram>();
	stringHistograms = new HashMap<String, StringHistogram>();
	minMaxMap = new HashMap<String, MinMax>();

	DbFileIterator iterator = databaseFile.iterator(null);
	try {
	    iterator.open();

	    while (iterator.hasNext()) {
		Tuple tuple = iterator.next();
		for (int i = 0; i < td.numFields(); i++) {
		    IntField f = (IntField) tuple.getField(i);
		    String name = td.getFieldName(i);
		    int v = f.getValue();
		    if (f.getType().equals(Type.INT_TYPE)) {
			MinMax m = minMaxMap.get(name);
			if (m == null) {
			    m = new MinMax();
			    m.min = v;
			    m.max = v;
			} else {
			    if (m.min > v)
				m.min = v;
			    if (m.max < v)
				m.max = v;
			}
			minMaxMap.put(name, m);
		    }
		}
	    }

	    iterator.rewind();
	    while (iterator.hasNext()) {
		num_tuples++;
		Tuple tuple = iterator.next();
		for (int i = 0; i < td.numFields(); i++) {
		    Field f = tuple.getField(i);
		    String name = td.getFieldName(i);
		    if (f.getType().equals(Type.INT_TYPE)) {
			IntHistogram g = intHistograms.get(name);
			if (g == null) // not exist
			    g = new IntHistogram(NUM_HIST_BINS,
				    minMaxMap.get(name).min,
				    minMaxMap.get(name).max);
			g.addValue(((IntField) f).getValue());
			intHistograms.put(td.getFieldName(i), g);
		    } else {
			// string type
			StringHistogram g = stringHistograms.get(td
				.getFieldName(i));
			if (g == null) // not exist
			    g = new StringHistogram(NUM_HIST_BINS);
			g.addValue(((StringField) f).getValue());
			stringHistograms.put(td.getFieldName(i), g);
		    }
		}
	    }
	}
	catch (DbException | TransactionAbortedException e) {
	    e.printStackTrace();
	}
	finally {
	    iterator.close();
	}
    }

    /**
     * The average selectivity of the field under op.
     * 
     * @param field
     *            the index of the field
     * @param op
     *            the operator in the predicate The semantic of the method is
     *            that, given the table, and then given a tuple, of which we do
     *            not know the value of the field, return the expected
     *            selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(final int field, final Predicate.Op op) {
	// TODO: some code goes here
	return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(final int field, final Predicate.Op op,
	    final Field constant) {
	Type fieldType = td.getFieldType(field);
	if (fieldType.equals(Type.INT_TYPE)) {
	    IntHistogram intHistogram = intHistograms.get(td
		    .getFieldName(field));
	    double value = intHistogram.estimateSelectivity(op,
		    ((IntField) constant).getValue());
	    return value;
	} else {// String Type
	    StringHistogram stringHistogram = stringHistograms.get(td
		    .getFieldName(field));
	    double value = stringHistogram.estimateSelectivity(op,
		    ((StringField) constant).getValue());
	    return value;
	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
	return num_tuples;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(final double selectivityFactor) {
	return (int) (num_tuples * selectivityFactor);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
	return ((HeapFile) databaseFile).numPages() * ioCostPerPage;
    }
}
