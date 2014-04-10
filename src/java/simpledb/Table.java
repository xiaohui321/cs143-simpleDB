package simpledb;

/**
 * @author zhouxiaohui TianruiZhang Helper Class to hold information for each
 *         table
 */
public class Table {

    private String name;
    private String pkeyField;
    private DbFile file;

    public Table(final String name, final String pkeyField, final DbFile file) {
	this.name = name;
	this.pkeyField = pkeyField;
	this.file = file;
    }

    public String getName() {
	return name;
    }

    public void setName(final String name) {
	this.name = name;
    }

    public String getPkeyField() {
	return pkeyField;
    }

    public void setPkeyField(final String pkeyField) {
	this.pkeyField = pkeyField;
    }

    public DbFile getFile() {
	return file;
    }

    public void setFile(final DbFile file) {
	this.file = file;
    }
}
