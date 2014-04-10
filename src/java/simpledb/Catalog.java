package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas. For now, this is a stub catalog that must be populated
 * with tables by a user program before it can be used -- eventually, this
 * should be converted to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

	//Member fields
	private List<Table> tableList;
	
	
	/**
	 * Constructor. Creates a new, empty catalog.
	 */
	public Catalog() {
		tableList = new ArrayList<Table>();     
	}

	/**
	 * Add a new table to the catalog. This table's contents are stored in the
	 * specified DbFile.
	 * 
	 * @param file
	 *            the contents of the table to add; file.getId() is the
	 *            identifier of this file/tupledesc param for the calls
	 *            getTupleDesc and getFile
	 * 
	 * @param name
	 *            the name of the table -- may be an empty string. May not be
	 *            null. If a name conflict exists, use the last table to be
	 *            added as the table for a given name.
	 * 
	 * @param pkeyField
	 *           the name of the primary key field
	 */
	public void addTable(final DbFile file, final String name, final String pkeyField) throws DbException{
		if(name==null || file.equals(null)) throw new DbException("name is null");
		  int i;
		  for(i=tableList.size()-1; i>=0; i--){
			  if(tableList.get(i).getName().equals(name))
				  break;
		  }
		  if(i>=0){
		  tableList.get(i).setFile(file);
		  tableList.get(i).setName(name);
		  tableList.get(i).setPkeyField(pkeyField);
		  }
		  else
		  tableList.add(new Table(name, pkeyField, file));		
	}

	public void addTable(final DbFile file, final String name) {
		try {
			addTable(file, name, "");
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<Table> getAllTables(){
		return tableList;
	}
	/**
	 * Add a new table to the catalog. This table has tuples formatted using the
	 * specified TupleDesc and its contents are stored in the specified DbFile.
	 * 
	 * @param file
	 *            the contents of the table to add; file.getId() is the
	 *            identifier of this file/tupledesc param for the calls
	 *            getTupleDesc and getFile
	 */
	public void addTable(final DbFile file) {
		addTable(file, UUID.randomUUID().toString());
	}

	/**
	 * Return the id of the table with a specified name,
	 * 
	 * @throws NoSuchElementException
	 *             if the table doesn't exist
	 */
	public int getTableId(final String name) throws NoSuchElementException {
	    if(name==null) throw new NoSuchElementException("name is null");
		for(Table t: tableList){
			if(t.getName().equals(name))
				return t.getFile().getId();
		}
		throw new NoSuchElementException("No such "+name+" exist");
	}

	/**
	 * Returns the tuple descriptor (schema) of the specified table
	 * 
	 * @param tableid
	 *            The id of the table, as specified by the DbFile.getId()
	 *            function passed to addTable
	 * @throws NoSuchElementException
	 *             if the table doesn't exist
	 */
	public TupleDesc getTupleDesc(final int tableid)
			throws NoSuchElementException {
		return getDatabaseFile(tableid).getTupleDesc();
	}

	/**
	 * Returns the DbFile that can be used to read the contents of the specified
	 * table.
	 * 
	 * @param tableid
	 *            The id of the table, as specified by the DbFile.getId()
	 *            function passed to addTable
	 */
	public DbFile getDatabaseFile(final int tableid)
			throws NoSuchElementException {
		for(Table t: tableList){
			if(t.getFile().getId()==tableid)
				return t.getFile();
		}
		throw new NoSuchElementException("No such DbFile with id"+tableid);
	}

	public String getPrimaryKey(final int tableid) throws NoSuchElementException {
		for(Table t: tableList){
			if(t.getFile().getId()==tableid)
				return t.getPkeyField();
		}
		throw new NoSuchElementException("Haha--from ray"); 
	}

	
	public Iterator<Integer> tableIdIterator() {
		//TODO: integer iterator unsolved
		return null;
	}

	public String getTableName(final int id) throws NoSuchElementException{
		for(Table t: tableList){
			if(t.getFile().getId()==id)
				return t.getName();
		}
		throw new NoSuchElementException("Xixi--from ray");
	}

	/** Delete all tables from the catalog */
	public void clear() {
		tableList.clear();
	}

	/**
	 * Reads the schema from a file and creates the appropriate tables in the
	 * database.
	 * 
	 * @param catalogFile
	 */
	public void loadSchema(final String catalogFile) {
		String line = "";
		String baseFolder = new File(new File(catalogFile).getAbsolutePath())
				.getParent();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(
					catalogFile)));

			while ((line = br.readLine()) != null) {
				// assume line is of the format name (field type, field type,
				// ...)
				String name = line.substring(0, line.indexOf("(")).trim();
				// System.out.println("TABLE NAME: " + name);
				String fields = line.substring(line.indexOf("(") + 1,
						line.indexOf(")")).trim();
				String[] els = fields.split(",");
				ArrayList<String> names = new ArrayList<String>();
				ArrayList<Type> types = new ArrayList<Type>();
				String primaryKey = "";
				for (String e : els) {
					String[] els2 = e.trim().split(" ");
					names.add(els2[0].trim());
					if (els2[1].trim().toLowerCase().equals("int"))
						types.add(Type.INT_TYPE);
					else if (els2[1].trim().toLowerCase().equals("string"))
						types.add(Type.STRING_TYPE);
					else {
						System.out.println("Unknown type " + els2[1]);
						System.exit(0);
					}
					if (els2.length == 3)
						if (els2[2].trim().equals("pk"))
							primaryKey = els2[0].trim();
						else {
							System.out.println("Unknown annotation " + els2[2]);
							System.exit(0);
						}
				}
				Type[] typeAr = types.toArray(new Type[0]);
				String[] namesAr = names.toArray(new String[0]);
				TupleDesc t = new TupleDesc(typeAr, namesAr);
				HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name
						+ ".dat"), t);
				try {
					addTable(tabHf, name, primaryKey);
				} catch (DbException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				System.out.println("Added table : " + name + " with schema "
						+ t);
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IndexOutOfBoundsException e) {
			System.out.println("Invalid catalog entry : " + line);
			System.exit(0);
		}
	}
}
