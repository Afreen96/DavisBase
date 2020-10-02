import java.io.File;
import java.util.ArrayList;
// a class to perform show dbs query
public class ShowDb implements QueryInterface {
    public Result executeAQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("Database");
        ResultSet resultSet = ResultSet.CreateResultSet();
        resultSet.setColumns(columns);
        ArrayList<Record> records = GetDatabases();

        for(Record record : records){
            resultSet.addRecord(record);
        }

        return resultSet;
    }

    public boolean validateAQuery() {
        return true;
    }

    private ArrayList<Record> GetDatabases(){
        ArrayList<Record> records = new ArrayList<>();

        File baseData = new File(DatabaseDefinedConstants.DEFAULT_DATA_DIRNAME);

        for(File data : baseData.listFiles()){
            if(!data.isDirectory()) continue;
            Record record = Record.createNewRecord();
            record.assign("Database", Literal.CreateLiteral(String.format("\"%s\"", data.getName())));
            records.add(record);
        }

        return records;
    }
}
