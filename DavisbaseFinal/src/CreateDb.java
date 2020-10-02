import java.io.File;

//a class to create a new database
public class CreateDb implements QueryInterface {
    public String databaseName;

    public CreateDb(String databaseName){
        this.databaseName = databaseName;
    }
//to execute a query
    public Result executeAQuery() {
        File database = new File(Errors.getDatabasePath(this.databaseName));
        boolean isCreated = database.mkdir();

        if(!isCreated){
            System.out.println(String.format("ERROR(200): Unable to create database '%s'", this.databaseName));
            return null;
        }

        Result result = new Result(1);
        return result;
    }

//to validate a query
    public boolean validateAQuery() {
        boolean databaseExists = DatabaseUtilityHelper.getDatabaseUtilityHelper().checkIfDatabaseExists(this.databaseName);

        if(databaseExists){
            System.out.println(String.format("ERROR(104D): Database '%s' already exists", this.databaseName));
            return false;
        }

        return true;
    }
}
