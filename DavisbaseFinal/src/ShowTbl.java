import java.util.ArrayList;
//a class to perform show tables query
public class ShowTbl implements QueryInterface {

    public String databaseName;

    public ShowTbl(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result executeAQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("table_name");

        CheckCondition condition = CheckCondition.conditionCreate(String.format("database_name = '%s'", this.databaseName));
        ArrayList<CheckCondition> conditionList = new ArrayList<>();
        conditionList.add(condition);

        QueryInterface query = new SelectQ(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, columns, conditionList, false);
        if (query.validateAQuery()) {
            return query.executeAQuery();
        }

        return null;
    }

    @Override
    public boolean validateAQuery() {
        boolean databaseExists = DatabaseUtilityHelper.getDatabaseUtilityHelper().checkIfDatabaseExists(this.databaseName);
        if(!databaseExists){
            Errors.printMissingDatabaseError(this.databaseName);
        }
        return databaseExists;
    }
}
