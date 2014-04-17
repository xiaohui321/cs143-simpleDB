package simpledb;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId seqTransId;
    private String seqTableAlias;
    private int seqTableId;
    private final HeapFile seqFile;
    private HeapFileIterator seqFileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(final TransactionId tid, final int tableid,
	    final String tableAlias) {
	seqTransId = tid;
	seqTableId = tableid;
	seqTableAlias = tableAlias;
	seqFile = (HeapFile) Database.getCatalog().getDatabaseFile(seqTableId);
	seqFileIterator = null;
    }

    /**
     * @return return the table name of the table the operator scans. This
     *         should be the actual name of the table in the catalog of the
     *         database
     * */
    public String getTableName() {
	return Database.getCatalog().getTableName(seqTableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
	return seqTableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * 
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(final int tableid, final String tableAlias) {
	seqTableId = tableid;
	seqTableAlias = tableAlias;
    }

    public SeqScan(final TransactionId tid, final int tableid) {
	this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
	seqFileIterator = (HeapFileIterator) seqFile.iterator(seqTransId);
	seqFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
	return Database.getCatalog().getTupleDesc(seqTableId);
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
	return seqFileIterator.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException,
	    TransactionAbortedException, DbException {
	return seqFileIterator.next();
    }

    @Override
    public void close() {
	seqFileIterator.close();
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
	    TransactionAbortedException {
	seqFileIterator.rewind();
    }
}
