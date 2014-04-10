package simpledb;

/**
 * @author zhouxiaohui TianruiZhang
 * Helper Class to hold information for each table
 */
public class Table {

	private String name;
	private String pkeyField;
	private DbFile file;
	public Table(String name, String pkeyField, DbFile file){
		this.name = name;
		this.pkeyField = pkeyField;
		this.file = file;
	}
	
	public String getName(){
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPkeyField() {
		return pkeyField;
	}
	public void setPkeyField(String pkeyField) {
		this.pkeyField = pkeyField;
	}
	public DbFile getFile() {
		return file;
	}
	public void setFile(DbFile file) {
		this.file = file;
	}
}
