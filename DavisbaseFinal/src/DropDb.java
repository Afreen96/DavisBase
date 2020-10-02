import java.io.File;
import java.util.ArrayList;
//class to implement drop database
public class DropDb implements QueryInterface {
    public String databaseName;

    public DropDb(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result executeAQuery() {
        File database = new File(Errors.getDatabasePath(databaseName));

        CheckCondition condition = CheckCondition.conditionCreate(String.format("database_name = '%s'", this.databaseName));
        ArrayList<CheckCondition> conditions = new ArrayList<>();
        conditions.add(condition);

        QueryInterface deleteEntryQuery = new DeleteQ(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, conditions, true);
        deleteEntryQuery.executeAQuery();

        deleteEntryQuery = new DeleteQ(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, conditions, true);
        deleteEntryQuery.executeAQuery();

        boolean isDeleted = Errors.RecursivelyDelete(database);

        if(!isDeleted){
            Errors.printMessage(String.format("ERROR(200): Unable to delete database '%s'", this.databaseName));
            return null;
        }

        if(QueryHandling.ActiveDatabaseName == this.databaseName){
            QueryHandling.ActiveDatabaseName = "";
        }

        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean validateAQuery() {
        boolean databaseExists = DatabaseUtilityHelper.getDatabaseUtilityHelper().checkIfDatabaseExists(this.databaseName);

        if(!databaseExists){
            Errors.printMissingDatabaseError(this.databaseName);
            return false;
        }

        return true;
    }
}
