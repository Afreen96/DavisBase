//an abstract class and enum to define datatype basis
enum Enum_DT {
    TINY_INT_NULL,
    SMALL_INT_NULL,
    INT_REAL_NULL,
    DOUBLE_DATETIME_NULL,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    REAL,
    DOUBLE,
    DATETIME,
    DATE,
    TEXT
}

public abstract class DataType<T> {

    protected T value;

    protected boolean isNull;

    protected final byte valueSerialCode;

    protected final byte nullSerialCode;

    public static DataType createDataType(Literal value) {
        switch(value.type) {
            case TINYINT:
                return new TinyInt_DT(Byte.valueOf(value.value));
            case SMALLINT:
                return new SmallInt_DT(Short.valueOf(value.value));
            case BIGINT:
                return new BigInt_DT(Long.valueOf(value.value));
            case INT:
                return new Int_DT(Integer.valueOf(value.value));
            case REAL:
                return new Real_DT(Float.valueOf(value.value));
            case DOUBLE:
                return new Double_DT(Double.valueOf(value.value));
            case DATETIME:
                return new DateTime_DT(Errors.getDateEpoc(value.value, false));
            case DATE:
                return new Date_DT(Errors.getDateEpoc(value.value, true));
            case TEXT:
                return new Text_DT(value.value);
        }

        return null;
    }

    public static DataType createSystemDataType(String value, byte dataType) {
        switch(dataType) {
            case DatabaseDefinedConstants.TINYINT:
                return new TinyInt_DT(Byte.valueOf(value));
            case DatabaseDefinedConstants.SMALLINT:
                return new SmallInt_DT(Short.valueOf(value));
            case DatabaseDefinedConstants.BIGINT:
                return new BigInt_DT(Long.valueOf(value));
            case DatabaseDefinedConstants.INT:
                return new Int_DT(Integer.valueOf(value));
            case DatabaseDefinedConstants.REAL:
                return new Real_DT(Float.valueOf(value));
            case DatabaseDefinedConstants.DOUBLE:
                return new Double_DT(Double.valueOf(value));
            case DatabaseDefinedConstants.DATETIME:
                return new DateTime_DT(Errors.getDateEpoc(value, false));
            case DatabaseDefinedConstants.DATE:
                return new Date_DT(Errors.getDateEpoc(value, true));
            case DatabaseDefinedConstants.TEXT:
                return new Text_DT(value);
        }

        return null;
    }

    protected DataType(int valueSerialCode, int nullSerialCode) {
        this.valueSerialCode = (byte) valueSerialCode;
        this.nullSerialCode = (byte) nullSerialCode;
    }

    public T getValue() {
        return value;
    }

    public String getStringValue() {
        if(value == null) {
            return "NULL";
        }
        return value.toString();
    }

    public void setValue(T value) {
        this.value = value;
         if (value != null) {
             this.isNull = false;
         }
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public byte getValueSerialCode() {
        return valueSerialCode;
    }

    public byte getNullSerialCode() {
        return nullSerialCode;
    }
}

class ColumnTypeForTable {

    public String name;
    public Enum_DT type;
    public boolean isNull;

    public static ColumnTypeForTable createAColumn(String columnString){
        String primaryKeyString = "primary key";
        String notNullString = "not null";
        boolean isNull = true;
        if(columnString.toLowerCase().endsWith(primaryKeyString)){
            columnString = columnString.substring(0, columnString.length() - primaryKeyString.length()).trim();
        }
        else if(columnString.toLowerCase().endsWith(notNullString)){
            columnString = columnString.substring(0, columnString.length() - notNullString.length()).trim();
            isNull = false;
        }

        String[] parts = columnString.split(" ");
        String name;
        if(parts.length > 2){
            QueryHandling.commandUnrecognised(columnString, "Expected column format <name> <datatype> [PRIMARY KEY | NOT NULL]");
            return null;
        }

        if(parts.length > 1){
            name = parts[0].trim();
            Enum_DT type = getDataTypeOf(parts[1].trim());
            if(type == null){
                QueryHandling.commandUnrecognised(columnString, "Unrecognised data type " + parts[1]);
                return null;
            }

            return new ColumnTypeForTable(name, type, isNull);
        }

        QueryHandling.commandUnrecognised(columnString, "Expected column format <name> <datatype> [PRIMARY KEY | NOT NULL]");
        return null;
    }

    private static Enum_DT getDataTypeOf(String dataTypeString) {
        switch(dataTypeString){
            case "tinyint": return Enum_DT.TINYINT;
            case "smallint": return Enum_DT.SMALLINT;
            case "int": return Enum_DT.INT;
            case "bigint": return Enum_DT.BIGINT;
            case "real": return Enum_DT.REAL;
            case "double": return Enum_DT.DOUBLE;
            case "datetime": return Enum_DT.DATETIME;
            case "date": return Enum_DT.DATE;
            case "text": return Enum_DT.TEXT;
        }

        return null;
    }

    private ColumnTypeForTable(String name, Enum_DT type, boolean isNull) {
        this.name = name;
        this.type = type;
        this.isNull = isNull;
    }
}


