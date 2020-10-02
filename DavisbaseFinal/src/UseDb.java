//a class to implement use db query
public class UseDb implements QueryInterface {
    public String databaseName;

    public UseDb(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result executeAQuery() {
        QueryHandling.ActiveDatabaseName = this.databaseName;
        Errors.printMessage("Database changed");
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
