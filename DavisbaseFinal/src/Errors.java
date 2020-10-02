import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

//class to check for errors
public class Errors {

    public static String getDatabasePath(String databaseName) {
        return DatabaseDefinedConstants.DEFAULT_DATA_DIRNAME + "/" + databaseName;
    }

    public static void printMissingDatabaseError(String databaseName) {
        printMessage("ERROR(105D): The database '" + databaseName + "' does not exist");
    }

    public static void printMissingTableError(String database, String tableName) {
        printMessage("ERROR(105T): Table '" + database + "." + tableName + "' doesn't exist.");
    }

    public static void printDuplicateTableError(String database, String tableName) {
        printMessage("ERROR(104T): Table '" + database + "." + tableName + "' already exist.");
    }

    public static void printMessage(String str) {
        System.out.println(str);
    }

    public static void printUnknownColumnValueError(String columnName, String value) {
        printMessage(String.format("ERROR(101): Invalid value: '%s' for column '%s'", value, columnName));
    }

    public static byte resolveClass(Object object) {
        if(object.getClass().equals(TinyInt_DT.class)) {
            return DatabaseDefinedConstants.TINYINT;
        }
        else if(object.getClass().equals(SmallInt_DT.class)) {
            return DatabaseDefinedConstants.SMALLINT;
        }
        else if(object.getClass().equals(Int_DT.class)) {
            return DatabaseDefinedConstants.INT;
        }
        else if(object.getClass().equals(BigInt_DT.class)) {
            return DatabaseDefinedConstants.BIGINT;
        }
        else if(object.getClass().equals(Real_DT.class)) {
            return DatabaseDefinedConstants.REAL;
        }
        else if(object.getClass().equals(Double_DT.class)) {
            return DatabaseDefinedConstants.DOUBLE;
        }
        else if(object.getClass().equals(Date_DT.class)) {
            return DatabaseDefinedConstants.DATE;
        }
        else if(object.getClass().equals(DateTime_DT.class)) {
            return DatabaseDefinedConstants.DATETIME;
        }
        else if(object.getClass().equals(Text_DT.class)) {
            return DatabaseDefinedConstants.TEXT;
        }
        else {
            return DatabaseDefinedConstants.INVALID_CLASS;
        }
    }

    static byte stringToDataType(String string) {
        if(string.compareToIgnoreCase("TINYINT") == 0) {
            return DatabaseDefinedConstants.TINYINT;
        }
        else if(string.compareToIgnoreCase("SMALLINT") == 0) {
            return DatabaseDefinedConstants.SMALLINT;
        }
        else if(string.compareToIgnoreCase("INT") == 0) {
            return DatabaseDefinedConstants.INT;
        }
        else if(string.compareToIgnoreCase("BIGINT") == 0) {
            return DatabaseDefinedConstants.BIGINT;
        }
        else if(string.compareToIgnoreCase("REAL") == 0) {
            return DatabaseDefinedConstants.REAL;
        }
        else if(string.compareToIgnoreCase("DOUBLE") == 0) {
            return DatabaseDefinedConstants.DOUBLE;
        }
        else if(string.compareToIgnoreCase("DATE") == 0) {
            return DatabaseDefinedConstants.DATE;
        }
        else if(string.compareToIgnoreCase("DATETIME") == 0) {
            return DatabaseDefinedConstants.DATETIME;
        }
        else if(string.compareToIgnoreCase("TEXT") == 0) {
            return DatabaseDefinedConstants.TEXT;
        }
        else {
            return DatabaseDefinedConstants.INVALID_CLASS;
        }
    }

    public static Enum_DT internalDataTypeToModelDataType(byte type) {
        switch (type) {
            case DatabaseDefinedConstants.TINYINT:
                return Enum_DT.TINYINT;
            case DatabaseDefinedConstants.SMALLINT:
                return Enum_DT.SMALLINT;
            case DatabaseDefinedConstants.INT:
                return Enum_DT.INT;
            case DatabaseDefinedConstants.BIGINT:
                return Enum_DT.BIGINT;
            case DatabaseDefinedConstants.REAL:
                return Enum_DT.REAL;
            case DatabaseDefinedConstants.DOUBLE:
                return Enum_DT.DOUBLE;
            case DatabaseDefinedConstants.DATE:
                return Enum_DT.DATE;
            case DatabaseDefinedConstants.DATETIME:
                return Enum_DT.DATETIME;
            case DatabaseDefinedConstants.TEXT:
                return Enum_DT.TEXT;
            default:
                return null;
        }
    }

    public static boolean isvalidDateFormat(String date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setLenient(false);
        try {
            formatter.parse(date);
        } catch (ParseException e) {
            return false;
        }

        return true;
    }

    public static boolean isvalidDateTimeFormat(String date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setLenient(false);
        try {
            formatter.parse(date);
        } catch (ParseException e) {
            return false;
        }

        return true;
    }

    public static Short ConvertFromOperator(CodesForOperator operator) {
        switch (operator){
            case EQUALS: return Num_DT.EQUALS;
            case GREATER_THAN_EQUAL: return Num_DT.GREATER_THAN_EQUALS;
            case GREATER_THAN: return Num_DT.GREATER_THAN;
            case LESS_THAN_EQUAL: return Num_DT.LESS_THAN_EQUALS;
            case LESS_THAN: return Num_DT.LESS_THAN;
        }

        return null;
    }

    public static boolean checkConditionValueDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, CheckCondition condition) {
        String invalidColumn = "";
        Literal literal = null;

        if (columnsList.contains(condition.column)) {
            int dataTypeIndex = columnDataTypeMapping.get(condition.column);
            literal = condition.value;

            if (literal.type != Errors.internalDataTypeToModelDataType((byte)dataTypeIndex)) {
                if (Errors.canUpdateLiteralDataType(literal, dataTypeIndex)) {
                    return true;
                }
            }
        }

        boolean valid = invalidColumn.length() <= 0;
        if (!valid) {
            Errors.printUnknownColumnValueError(invalidColumn, literal.value);
        }

        return valid;
    }

    public static long getDateEpoc(String value, Boolean isDate) {
        DateFormat formatter;
        if (isDate) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        formatter.setLenient(false);
        Date date;
        try {
            date = formatter.parse(value);

            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(),
                    ZoneId.systemDefault());

            return zdt.toInstant().toEpochMilli() / 1000;
        }
        catch (ParseException ex) {
            return 0;
        }
    }

    public static String getDateEpocAsString(long value, Boolean isDate) {
        ZoneId zoneId = ZoneId.of ("America/Chicago" );

        Instant i = Instant.ofEpochSecond (value);
        ZonedDateTime zdt2 = ZonedDateTime.ofInstant (i, zoneId);
        Date date = Date.from(zdt2.toInstant());

        DateFormat formatter;
        if (isDate) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        formatter.setLenient(false);

        return formatter.format(date);
    }

    public boolean checkDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, List<Literal> values) {
        String invalidColumn = "";
        Literal invalidLiteral = null;

        for (int i =0; i < values.size(); i++) {
            String columnName = columnsList.get(i);

            int dataTypeId = columnDataTypeMapping.get(columnName);

            int idx = columnsList.indexOf(columnName);
            Literal literal = values.get(idx);
            invalidLiteral = literal;

            if (literal.type != Errors.internalDataTypeToModelDataType((byte)dataTypeId)) {

                if (Errors.canUpdateLiteralDataType(literal, dataTypeId)) {
                    continue;
                }

                invalidColumn = columnName;
                break;
            }

            if (literal.type != Errors.internalDataTypeToModelDataType((byte)dataTypeId)) {
                invalidColumn = columnName;
                break;
            }
        }

        boolean valid = invalidColumn.length() <= 0;
        if (!valid) {
            Errors.printUnknownColumnValueError(invalidColumn, invalidLiteral.value);
            return false;
        }

        return true;
    }

    public static boolean RecursivelyDelete(File file){
        if(file == null) return true;
        boolean isDeleted;

        if(file.isDirectory()) {
            for (File childFile : file.listFiles()) {
                if (childFile.isFile()) {
                    isDeleted = childFile.delete();
                    if (!isDeleted) return false;
                } else {
                    isDeleted = RecursivelyDelete(childFile);
                    if (!isDeleted) return false;
                }
            }
        }

        return file.delete();
    }

    public static boolean canUpdateLiteralDataType(Literal literal, int columnType) {
        if (columnType == DatabaseDefinedConstants.TINYINT) {
            if (literal.type == Enum_DT.INT) {
                if (Integer.parseInt(literal.value) <= Byte.MAX_VALUE) {
                    literal.type = Enum_DT.TINYINT;
                    return true;
                }
            }
        } else if (columnType == DatabaseDefinedConstants.SMALLINT) {
            if (literal.type == Enum_DT.INT) {
                if (Integer.parseInt(literal.value) <= Short.MAX_VALUE) {
                    literal.type = Enum_DT.SMALLINT;
                    return true;
                }
            }
        } else if (columnType == DatabaseDefinedConstants.BIGINT) {
            if (literal.type == Enum_DT.INT) {
                if (Integer.parseInt(literal.value) <= Long.MAX_VALUE) {
                    literal.type = Enum_DT.BIGINT;
                    return true;
                }
            }
        } else if (columnType == DatabaseDefinedConstants.DOUBLE) {
            if (literal.type == Enum_DT.REAL) {
                literal.type = Enum_DT.DOUBLE;
                return true;
            }
        }
        return false;
    }
}
