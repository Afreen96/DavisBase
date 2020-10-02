import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

// a class to implement an internal record
class Record {
    HashMap<String, Literal> vMap;

    public static Record createNewRecord(){
        return new Record();
    }

    private Record(){
        this.vMap = new HashMap<>();
    }

    public void assign(String columnName, Literal value){
        if(columnName.length() == 0) return;
        if(value == null) return;

        this.vMap.put(columnName, value);
    }

    public String getValue(String column) {
        Literal literal = this.vMap.get(column);
        return literal.toString();
    }
}

class RecordPointer {

    private int pageNumber_Left;

    private int keyValue;

    private int pageNumber;

    private short offsetValue;

    public RecordPointer() {
        pageNumber_Left = -1;
        keyValue = -1;
        offsetValue = -1;
        pageNumber = -1;
    }

    public int getPageNumber_Left() {
        return pageNumber_Left;
    }

    public void setPageNumber_Left(int leftPageNumber) {
        this.pageNumber_Left = leftPageNumber;
    }

    public int getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(int key) {
        this.keyValue = key;
    }

    public int getSizeOf() {
        return Integer.BYTES + Integer.BYTES;
    }

    public short getOffsetValue() {
        return offsetValue;
    }

    public void setOffsetValue(short offset) {
        this.offsetValue = offset;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }
}

class DataRecord {

    private List<Object> valuesOfColumns;

    private short size;

    private int rowId;

    private int locationOfPage;

    private short offsetVal;

    public DataRecord() {
        size = 0;
        valuesOfColumns = new ArrayList<>();
        locationOfPage = -1;
        offsetVal = -1;
    }

    public List<Object> getValuesOfColumns() {
        return valuesOfColumns;
    }

    public short getSizeOf() {
        return size;
    }

    public void setSizeOf(short size) {
        this.size = size;
    }

    public short getSizeOfHeader() {
        return (short)(Short.BYTES + Integer.BYTES);
    }

    public void assignSizeTo() {
        this.size = (short) (this.valuesOfColumns.size() + 1);
        for(Object object: valuesOfColumns) {
            if(object.getClass().equals(TinyInt_DT.class)) {
                this.size += ((TinyInt_DT) object).getSIZE();
            }
            else if(object.getClass().equals(SmallInt_DT.class)) {
                this.size += ((SmallInt_DT) object).getSIZE();
            }
            else if(object.getClass().equals(Int_DT.class)) {
                this.size += ((Int_DT) object).getSIZE();
            }
            else if(object.getClass().equals(BigInt_DT.class)) {
                this.size += ((BigInt_DT) object).getSIZE();
            }
            else if(object.getClass().equals(Real_DT.class)) {
                this.size += ((Real_DT) object).getSIZE();
            }
            else if(object.getClass().equals(Double_DT.class)) {
                this.size += ((Double_DT) object).getSIZE();
            }
            else if(object.getClass().equals(DateTime_DT.class)) {
                size += ((DateTime_DT) object).getSIZE();
            }
            else if(object.getClass().equals(Date_DT.class)) {
                this.size += ((Date_DT) object).getSIZE();
            }
            else if(object.getClass().equals(Text_DT.class)) {
                this.size += ((Text_DT) object).getSize();
            }
        }
    }

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    public int getLocationOfPage() {
        return locationOfPage;
    }

    public void setLocationOfPage(int pageLocated) {
        this.locationOfPage = pageLocated;
    }

    public short getOffsetVal() {
        return offsetVal;
    }

    public void setOffsetVal(short offset) {
        this.offsetVal = offset;
    }

    public byte[] getSerialTypeCodes() {
        byte[] serialTypeCodes = new byte[valuesOfColumns.size()];
        byte index = 0;
        for(Object object: valuesOfColumns) {
            switch (Errors.resolveClass(object)) {
                case DatabaseDefinedConstants.TINYINT:
                    serialTypeCodes[index++] = ((TinyInt_DT) object).getSerialCode();
                    break;

                case DatabaseDefinedConstants.SMALLINT:
                    serialTypeCodes[index++] = ((SmallInt_DT) object).getSerialCode();
                    break;

                case DatabaseDefinedConstants.INT:
                    serialTypeCodes[index++] = ((Int_DT) object).getSerialCode();
                    break;

                case DatabaseDefinedConstants.BIGINT:
                    serialTypeCodes[index++] = ((BigInt_DT) object).getSerialCode();
                    break;

                case DatabaseDefinedConstants.REAL:
                    serialTypeCodes[index++] = ((Real_DT) object).getSerialCode();
                    break;

                case DatabaseDefinedConstants.DOUBLE:
                    serialTypeCodes[index++] = ((Double_DT) object).getSerialCode();
                    break;

                case DatabaseDefinedConstants.DATETIME:
                    serialTypeCodes[index++] = ((DateTime_DT) object).getSerialCode();
                    break;

                case DatabaseDefinedConstants.DATE:
                    serialTypeCodes[index++] = ((Date_DT) object).getSerialCode();
                    break;

                case DatabaseDefinedConstants.TEXT:
                    serialTypeCodes[index++] = ((Text_DT) object).getSerialCode();
                    break;
            }
        }
        return serialTypeCodes;
    }
}

