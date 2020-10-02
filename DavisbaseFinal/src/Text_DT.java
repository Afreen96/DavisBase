// a base class for text datatype
public class Text_DT extends DataType<String> {
    public Text_DT() {
        this("", true);
    }

    public Text_DT(String value) {
        this(value, value == null);
    }

    public Text_DT(String value, boolean isNull) {
        super(DatabaseDefinedConstants.TEXT_SERIAL_TYPE_CODE, DatabaseDefinedConstants.ONE_BYTE_NULL_SERIAL_TYPE_CODE);
        this.value = value;
        this.isNull = isNull;
    }

    public byte getSerialCode() {
        if(isNull)
            return nullSerialCode;
        else
            return (byte)(valueSerialCode + this.value.length());
    }

    public int getSize() {
        if(isNull)
            return 0;
        return this.value.length();
    }
}
