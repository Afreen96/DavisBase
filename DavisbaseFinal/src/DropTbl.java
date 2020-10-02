import java.io.File;
import java.util.ArrayList;
//class to implement drop table
public class DropTbl implements QueryInterface {

    public String databaseName;
    public String tableName;

    public DropTbl(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public Result executeAQuery() {

        ArrayList<CheckCondition> conditionList = new ArrayList<>();
        conditionList.add(CheckCondition.conditionCreate(String.format("database_name = '%s'", this.databaseName)));
        conditionList.add(CheckCondition.conditionCreate(String.format("table_name = '%s'", this.tableName)));

        QueryInterface DeleteQ = new DeleteQ(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, conditionList, true);
        DeleteQ.executeAQuery();

        DeleteQ  = new DeleteQ(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, conditionList, true);
        DeleteQ.executeAQuery();

        File table = new File(String.format("%s/%s/%s%s", DatabaseDefinedConstants.DEFAULT_DATA_DIRNAME, this.databaseName, this.tableName, DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION));

        if(!Errors.RecursivelyDelete(table)){
            Errors.printMessage(String.format("ERROR(200): Unable to delete table '%s.%s'", this.databaseName, this.tableName));
            return null;
        }
        return new Result(1);
    }

    @Override
    public boolean validateAQuery() {
        if(!DatabaseUtilityHelper.getDatabaseUtilityHelper().checkIfTableExists(this.databaseName, this.tableName)){
            Errors.printMissingTableError(this.databaseName, this.tableName);
            return false;
        }

        return true;
    }
}
