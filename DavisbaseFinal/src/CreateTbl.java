import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//class that creates new table in the database
public class CreateTbl implements QueryInterface {
    public String tableName;
    public ArrayList<ColumnTypeForTable> columns;
    private boolean hasPrimaryKey;
    public String databaseName;

    public CreateTbl(String databaseName, String tableName, ArrayList<ColumnTypeForTable> columns, boolean hasPrimaryKey){
        this.tableName = tableName;
        this.columns = columns;
        this.hasPrimaryKey = hasPrimaryKey;
        this.databaseName = databaseName;
    }

    @Override
    public Result executeAQuery() {
        return new Result(1);
    }

    @Override
    public boolean validateAQuery() {
        try {
            IO_Handling IO_Handling = new IO_Handling();

            if (!IO_Handling.checkDbExists(this.databaseName)) {
                Errors.printMissingDatabaseError(databaseName);
                return false;
            }

            if (IO_Handling.checkTblExists(this.databaseName, tableName)) {
                Errors.printDuplicateTableError(this.databaseName, tableName);
                return false;
            }

            if (checkDuplicateColumns(columns)) {
                Errors.printMessage("ERROR(102C): Table cannot have duplicate columns.");
                return false;
            }


            List<InternalColumn> columnsList = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                InternalColumn internalColumn = new InternalColumn();

                ColumnTypeForTable column = columns.get(i);
                internalColumn.setName(column.name);
                internalColumn.setDataType(column.type.toString());

                if (hasPrimaryKey && i == 0) {
                    internalColumn.setPrimary(true);
                } else {
                    internalColumn.setPrimary(false);
                }

                if (hasPrimaryKey && i == 0) {
                    internalColumn.setNullable(false);
                } else if (column.isNull) {
                    internalColumn.setNullable(true);
                } else {
                    internalColumn.setNullable(false);
                }

                columnsList.add(internalColumn);
            }

            boolean status = IO_Handling.createNewTable(this.databaseName, tableName + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION);
            if (status) {
                DatabaseUtility databaseHelper = new DatabaseUtility();
                int startingRowId = databaseHelper.updateInternalTable(this.databaseName, tableName, columns.size());
                boolean systemTableUpdateStatus = databaseHelper.updateInternalColumns(this.databaseName, tableName, startingRowId, columnsList);

                if (!systemTableUpdateStatus) {
                    Errors.printMessage("ERROR(102T): Failed to create table " + tableName);
                    return false;
                }
            }
        }
        catch (InternalException e) {
            Errors.printMessage(e.getMessage());
            return false;
        }

        return true;
    }

    private boolean checkDuplicateColumns(ArrayList<ColumnTypeForTable> columnArrayList) {
        HashMap<String, Integer> map = new HashMap<>();
        for (int i = 0; i < columnArrayList.size(); i++) {
            ColumnTypeForTable column = columnArrayList.get(i);
            if (map.containsKey(column.name)) {
                return true;
            }
            else {
                map.put(column.name, i);
            }
        }

        return false;
    }
}
