import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

//class to process input from IO to carry out all IO operations on the database
public class IO_Handling {

    public boolean checkDbExists(String databaseName) {
        File databaseDir = new File(Errors.getDatabasePath(databaseName));
        return  databaseDir.exists();
    }

    public boolean createNewTable(String databaseName, String tableName) throws InternalException {
        try {
            File dirFile = new File(Errors.getDatabasePath(databaseName));
            if (!dirFile.exists()) {
                dirFile.mkdir();
            }
            File file = new File(Errors.getDatabasePath(databaseName) + "/" + tableName);
            if (file.exists()) {
                return false;
            }
            if (file.createNewFile()) {
                RandomAccessFile randomAccessFile;
                Page<DataRecord> page = Page.createNewEmptyPage(new DataRecord());
                randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(Page.PAGE_SIZE);
                boolean isTableCreated = writePageHeader(randomAccessFile, page);
                randomAccessFile.close();
                return isTableCreated;
            }
            return false;
        } catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    public boolean checkTblExists(String databaseName, String tableName) {
        boolean databaseExists = this.checkDbExists(databaseName);
        boolean fileExists = new File(Errors.getDatabasePath(databaseName) + "/" + tableName + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION).exists();

        return (databaseExists && fileExists);
    }

    public boolean writeNewRecord(String databaseName, String tableName, DataRecord record) throws InternalException {
        RandomAccessFile randomAccessFile;
        try {
            File file = new File(Errors.getDatabasePath(databaseName) + "/" + tableName + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                randomAccessFile = new RandomAccessFile(file, "rw");
                Page page = getPage(randomAccessFile, record, 0);
                if (page == null) return false;
                if (!checkSpaceRequirements(page, record)) {
                    int pageCount = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                    switch (pageCount) {
                        case 1:
                            RecordPointer pointerRecord = splitPage(randomAccessFile, page, record, 1, 2);
                            Page<RecordPointer> pointerRecordPage = Page.createNewEmptyPage(pointerRecord);
                            pointerRecordPage.setPageNumber(0);
                            pointerRecordPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                            pointerRecordPage.setNumberOfCells((byte) 1);
                            pointerRecordPage.setStartingAddress((short) (pointerRecordPage.getStartingAddress() - pointerRecord.getSizeOf()));
                            pointerRecordPage.setRightNodeAddress(2);
                            pointerRecordPage.getRecordAddressList().add((short) (pointerRecordPage.getStartingAddress() + 1));
                            pointerRecord.setPageNumber(pointerRecordPage.getPageNumber());
                            pointerRecord.setOffsetValue((short) (pointerRecordPage.getStartingAddress() + 1));
                            this.writePageHeader(randomAccessFile, pointerRecordPage);
                            this.writeRecord(randomAccessFile, pointerRecord);
                            break;

                        default:
                            if(pageCount > 1) {
                                RecordPointer pointerRecord1 = splitPage(randomAccessFile, readPageHeader(randomAccessFile, 0), record);
                                if(pointerRecord1 != null && pointerRecord1.getPageNumber_Left() != -1)  {
                                    Page<RecordPointer> rootPage = Page.createNewEmptyPage(pointerRecord1);
                                    rootPage.setPageNumber(0);
                                    rootPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                                    rootPage.setNumberOfCells((byte) 1);
                                    rootPage.setStartingAddress((short)(rootPage.getStartingAddress() - pointerRecord1.getSizeOf()));
                                    rootPage.setRightNodeAddress(pointerRecord1.getPageNumber());
                                    rootPage.getRecordAddressList().add((short) (rootPage.getStartingAddress() + 1));
                                    pointerRecord1.setOffsetValue((short) (rootPage.getStartingAddress() + 1));
                                    this.writePageHeader(randomAccessFile, rootPage);
                                    this.writeRecord(randomAccessFile, pointerRecord1);
                                }
                            }
                            break;
                    }
                    DatabaseUtility.incrementTableRowCount(databaseName, tableName);
                    randomAccessFile.close();
                    return true;
                }
                short address = (short) getAddress(file, record.getRowId(), page.getPageNumber());
                page.setNumberOfCells((byte)(page.getNumberOfCells() + 1));
                page.setStartingAddress((short) (page.getStartingAddress() - record.getSizeOf() - record.getSizeOfHeader()));
                if(address == page.getRecordAddressList().size())
                    page.getRecordAddressList().add((short)(page.getStartingAddress() + 1));
                else
                    page.getRecordAddressList().add(address, (short)(page.getStartingAddress() + 1));
                record.setLocationOfPage(page.getPageNumber());
                record.setOffsetVal((short) (page.getStartingAddress() + 1));
                this.writePageHeader(randomAccessFile, page);
                this.writeRecord(randomAccessFile, record);
                DatabaseUtility.incrementTableRowCount(databaseName, tableName);
                randomAccessFile.close();
            } else {
                Errors.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
            }
            return true;
        } catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private boolean checkSpaceRequirements(Page page, DataRecord record) {
        if (page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSizeOf() + record.getSizeOfHeader() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    private boolean checkSpaceRequirements(Page page, RecordPointer record) {
        if(page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSizeOf() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    private RecordPointer splitPage(RandomAccessFile randomAccessFile, Page page, DataRecord record, int pageNumber1, int pageNumber2) throws InternalException {
        try {
            if (page != null && record != null) {
                int location;
                RecordPointer pointerRecord = new RecordPointer();
                if (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType());
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));
                if (location == page.getNumberOfCells()) {
                    Page<DataRecord> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setNumberOfCells(page.getNumberOfCells());
                    page1.setRightNodeAddress(pageNumber2);
                    page1.setStartingAddress(page.getStartingAddress());
                    page1.setRecordAddressList(page.getRecordAddressList());
                    this.writePageHeader(randomAccessFile, page1);
                    List<DataRecord> records = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, page.getNumberOfCells(), page1.getPageNumber(), record);
                    for (DataRecord object : records) {
                        this.writeRecord(randomAccessFile, object);
                    }
                    Page<DataRecord> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    page2.setNumberOfCells((byte) 1);
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    page2.setStartingAddress((short) (page2.getStartingAddress() - record.getSizeOf() - record.getSizeOfHeader()));
                    page2.getRecordAddressList().add((short) (page2.getStartingAddress() + 1));
                    this.writePageHeader(randomAccessFile, page2);
                    record.setLocationOfPage(page2.getPageNumber());
                    record.setOffsetVal((short) (page2.getStartingAddress() + 1));
                    this.writeRecord(randomAccessFile, record);
                    pointerRecord.setKeyValue(record.getRowId());
                } else {
                    boolean isFirst = false;
                    if (location < (page.getRecordAddressList().size() / 2)) {
                        isFirst = true;
                    }
                    randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));

                    //Page 1
                    Page<DataRecord> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setPageNumber(pageNumber1);
                    List<DataRecord> leftRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, (byte) (page.getNumberOfCells() / 2), page1.getPageNumber(), record);
                    if (isFirst) {
                        record.setLocationOfPage(page1.getPageNumber());
                        leftRecords.add(location, record);
                    }
                    page1.setNumberOfCells((byte) leftRecords.size());
                    int index = 0;
                    short offset = Page.PAGE_SIZE;
                    for (DataRecord dataRecord : leftRecords) {
                        index++;
                        offset = (short) (Page.PAGE_SIZE - ((dataRecord.getSizeOf() + dataRecord.getSizeOfHeader()) * index));
                        dataRecord.setOffsetVal(offset);
                        page1.getRecordAddressList().add(offset);
                    }
                    page1.setStartingAddress((short) (offset - 1));
                    page1.setRightNodeAddress(pageNumber2);
                    this.writePageHeader(randomAccessFile, page1);
                    for(DataRecord dataRecord : leftRecords) {
                        this.writeRecord(randomAccessFile, dataRecord);
                    }

                    //Page 2
                    Page<DataRecord> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    List<DataRecord> rightRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) ((page.getNumberOfCells() / 2) + 1), page.getNumberOfCells(), pageNumber2, record);
                    if(!isFirst) {
                        record.setLocationOfPage(page2.getPageNumber());
                        int position = (location - (page.getRecordAddressList().size() / 2) + 1);
                        if(position >= rightRecords.size())
                            rightRecords.add(record);
                        else
                            rightRecords.add(position, record);
                    }
                    page2.setNumberOfCells((byte) rightRecords.size());
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    pointerRecord.setKeyValue(rightRecords.get(0).getRowId());
                    index = 0;
                    offset = Page.PAGE_SIZE;
                    for(DataRecord dataRecord : rightRecords) {
                        index++;
                        offset = (short) (Page.PAGE_SIZE - ((dataRecord.getSizeOf() + dataRecord.getSizeOfHeader()) * index));
                        dataRecord.setOffsetVal(offset);
                        page2.getRecordAddressList().add(offset);
                    }
                    page2.setStartingAddress((short) (offset - 1));
                    this.writePageHeader(randomAccessFile, page2);
                    for(DataRecord dataRecord : rightRecords) {
                        this.writeRecord(randomAccessFile, dataRecord);
                    }
                }
                pointerRecord.setPageNumber_Left(pageNumber1);
                return pointerRecord;
            }
        } catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return null;
    }

    private RecordPointer splitPage(RandomAccessFile randomAccessFile, Page page, DataRecord record) throws InternalException {
        try {
            if (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                int pageNumber = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
                Page newPage = this.readPageHeader(randomAccessFile, pageNumber);
                RecordPointer pointerRecord = splitPage(randomAccessFile, newPage, record);
                if (pointerRecord.getPageNumber() == -1)
                    return pointerRecord;
                if (checkSpaceRequirements(page, pointerRecord)) {
                    int location = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE, true);
                    page.setNumberOfCells((byte) (page.getNumberOfCells() + 1));
                    page.setStartingAddress((short) (page.getStartingAddress() - pointerRecord.getSizeOf()));
                    page.getRecordAddressList().add(location, (short) (page.getStartingAddress() + 1));
                    page.setRightNodeAddress(pointerRecord.getPageNumber());
                    pointerRecord.setPageNumber(page.getPageNumber());
                    pointerRecord.setOffsetValue((short) (page.getStartingAddress() + 1));
                    this.writePageHeader(randomAccessFile, page);
                    this.writeRecord(randomAccessFile, pointerRecord);
                    return new RecordPointer();
                } else {
                    int newPageNumber = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                    page.setRightNodeAddress(pointerRecord.getPageNumber());
                    this.writePageHeader(randomAccessFile, page);
                    RecordPointer pointerRecord1 = splitPage(randomAccessFile, page, pointerRecord, page.getPageNumber(), newPageNumber);
                    return pointerRecord1;
                }
            } else if (page.getPageType() == Page.LEAF_TABLE_PAGE) {
                int newPageNumber = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                RecordPointer pointerRecord = splitPage(randomAccessFile, page, record, page.getPageNumber(), newPageNumber);
                if (pointerRecord != null)
                    pointerRecord.setPageNumber(newPageNumber);
                return pointerRecord;
            }
            return null;
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private RecordPointer splitPage(RandomAccessFile randomAccessFile, Page page, RecordPointer record, int pageNumber1, int pageNumber2) throws InternalException {
        try {
            if (page != null && record != null) {
                int location;
                boolean isFirst = false;

                RecordPointer pointerRecord;
                if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getKeyValue(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType(), true);
                if (location < (page.getRecordAddressList().size() / 2)) {
                    isFirst = true;
                }

                if(pageNumber1 == 0) {
                    pageNumber1 = pageNumber2;
                    pageNumber2++;
                }
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));

                //Page 1
                Page<RecordPointer> page1 = new Page<>(pageNumber1);
                page1.setPageType(page.getPageType());
                page1.setPageNumber(pageNumber1);
                List<RecordPointer> leftRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, (byte) (page.getNumberOfCells() / 2), page1.getPageNumber(), record);
                if (isFirst)
                    leftRecords.add(location, record);
                pointerRecord = leftRecords.get(leftRecords.size() - 1);
                pointerRecord.setPageNumber(pageNumber2);
                leftRecords.remove(leftRecords.size() - 1);
                page1.setNumberOfCells((byte) leftRecords.size());
                int index = 0;
                short offset = Page.PAGE_SIZE;
                for (RecordPointer pointerRecord1 : leftRecords) {
                    index++;
                    offset = (short) (Page.PAGE_SIZE - (pointerRecord1.getSizeOf() * index));
                    pointerRecord1.setOffsetValue(offset);
                    page1.getRecordAddressList().add(offset);
                }
                page1.setStartingAddress((short) (offset - 1));
                page1.setRightNodeAddress(pointerRecord.getPageNumber_Left());
                this.writePageHeader(randomAccessFile, page1);
                for(RecordPointer pointerRecord1 : leftRecords) {
                    this.writeRecord(randomAccessFile, pointerRecord1);
                }

                //Page 2
                Page<RecordPointer> page2 = new Page<>(pageNumber2);
                page2.setPageType(page.getPageType());
                List<RecordPointer> rightRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) ((page.getNumberOfCells() / 2) + 1), page.getNumberOfCells(), pageNumber2, record);
                if(!isFirst) {
                    int position = (location - (page.getRecordAddressList().size() / 2) + 1);
                    if(position >= rightRecords.size())
                        rightRecords.add(record);
                    else
                        rightRecords.add(position, record);
                }
                page2.setNumberOfCells((byte) rightRecords.size());
                page2.setRightNodeAddress(page.getRightNodeAddress());
                rightRecords.get(0).setPageNumber_Left(page.getRightNodeAddress());
                index = 0;
                offset = Page.PAGE_SIZE;
                for(RecordPointer pointerRecord1 : rightRecords) {
                    index++;
                    offset = (short) (Page.PAGE_SIZE - (pointerRecord1.getSizeOf() * index));
                    pointerRecord1.setOffsetValue(offset);
                    page2.getRecordAddressList().add(offset);
                }
                page2.setStartingAddress((short) (offset - 1));
                this.writePageHeader(randomAccessFile, page2);
                for(RecordPointer pointerRecord1 : rightRecords) {
                    this.writeRecord(randomAccessFile, pointerRecord1);
                }
                pointerRecord.setPageNumber(pageNumber2);
                pointerRecord.setPageNumber_Left(pageNumber1);
                return pointerRecord;
            }
        } catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return null;
    }

    private <T> List<T> copyRecords(RandomAccessFile randomAccessFile, long pageStartAddress, List<Short> recordAddresses, byte startIndex, byte endIndex, int pageNumber, T object) throws InternalException {
        try {
            List<T> records = new ArrayList<>();
            byte numberOfRecords;
            byte[] serialTypeCodes;
            for (byte i = startIndex; i < endIndex; i++) {
                randomAccessFile.seek(pageStartAddress + recordAddresses.get(i));
                if (object.getClass().equals(RecordPointer.class)) {
                    RecordPointer record = new RecordPointer();
                    record.setPageNumber(pageNumber);
                    record.setOffsetValue((short) (pageStartAddress + Page.PAGE_SIZE - 1 - (record.getSizeOf() * (i - startIndex + 1))));
                    record.setPageNumber_Left(randomAccessFile.readInt());
                    record.setKeyValue(randomAccessFile.readInt());
                    records.add(i - startIndex, (T) record);
                } else if (object.getClass().equals(DataRecord.class)) {
                    DataRecord record = new DataRecord();
                    record.setLocationOfPage(pageNumber);
                    record.setOffsetVal(recordAddresses.get(i));
                    record.setSizeOf(randomAccessFile.readShort());
                    record.setRowId(randomAccessFile.readInt());
                    numberOfRecords = randomAccessFile.readByte();
                    serialTypeCodes = new byte[numberOfRecords];
                    for (byte j = 0; j < numberOfRecords; j++) {
                        serialTypeCodes[j] = randomAccessFile.readByte();
                    }
                    for (byte j = 0; j < numberOfRecords; j++) {
                        switch (serialTypeCodes[j]) {
                            //case TinyInt_DT.nullSerialCode is overridden with Text_DT

                            case DatabaseDefinedConstants.ONE_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new Text_DT(null));
                                break;

                            case DatabaseDefinedConstants.TWO_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new SmallInt_DT(randomAccessFile.readShort(), true));
                                break;

                            case DatabaseDefinedConstants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new Real_DT(randomAccessFile.readFloat(), true));
                                break;

                            case DatabaseDefinedConstants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new Double_DT(randomAccessFile.readDouble(), true));
                                break;

                            case DatabaseDefinedConstants.TINY_INT_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new TinyInt_DT(randomAccessFile.readByte()));
                                break;

                            case DatabaseDefinedConstants.SMALL_INT_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new SmallInt_DT(randomAccessFile.readShort()));
                                break;

                            case DatabaseDefinedConstants.INT_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new Int_DT(randomAccessFile.readInt()));
                                break;

                            case DatabaseDefinedConstants.BIG_INT_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new BigInt_DT(randomAccessFile.readLong()));
                                break;

                            case DatabaseDefinedConstants.REAL_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new Real_DT(randomAccessFile.readFloat()));
                                break;

                            case DatabaseDefinedConstants.DOUBLE_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new Double_DT(randomAccessFile.readDouble()));
                                break;

                            case DatabaseDefinedConstants.DATE_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new Date_DT(randomAccessFile.readLong()));
                                break;

                            case DatabaseDefinedConstants.DATE_TIME_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new DateTime_DT(randomAccessFile.readLong()));
                                break;

                            case DatabaseDefinedConstants.TEXT_SERIAL_TYPE_CODE:
                                record.getValuesOfColumns().add(new Text_DT(""));
                                break;

                            default:
                                if (serialTypeCodes[j] > DatabaseDefinedConstants.TEXT_SERIAL_TYPE_CODE) {
                                    byte length = (byte) (serialTypeCodes[j] - DatabaseDefinedConstants.TEXT_SERIAL_TYPE_CODE);
                                    char[] text = new char[length];
                                    for (byte k = 0; k < length; k++) {
                                        text[k] = (char) randomAccessFile.readByte();
                                    }
                                    record.getValuesOfColumns().add(new Text_DT(new String(text)));
                                }
                                break;

                        }
                    }
                    records.add(i - startIndex, (T) record);
                }
            }
            return records;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private Page getPage(RandomAccessFile randomAccessFile, DataRecord record, int pageNumber) throws InternalException {
        try {
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if (page.getPageType() == Page.LEAF_TABLE_PAGE) {
                return page;
            }
            pageNumber = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
            if (pageNumber == -1) return null;
            return getPage(randomAccessFile, record, pageNumber);
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private int getAddress(File file, int rowId, int pageNumber) throws InternalException {
        int location = -1;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                location = binarySearch(randomAccessFile, rowId, page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.LEAF_TABLE_PAGE);
                randomAccessFile.close();
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return location;
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType) throws InternalException {
        return binarySearch(randomAccessFile, key, numberOfRecords, seekPosition, pageType, false);
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType, boolean literalSearch) throws InternalException {
        try {
            int start = 0, end = numberOfRecords;
            int mid;
            int pageNumber = -1;
            int rowId;
            short address;

            while(true) {
                if(start > end || start == numberOfRecords) {
                    if(pageType == Page.LEAF_TABLE_PAGE || literalSearch)
                        return start > numberOfRecords ? numberOfRecords : start;
                    if(pageType == Page.INTERIOR_TABLE_PAGE) {
                        if (end < 0)
                            return pageNumber;
                        randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + 4);
                        return randomAccessFile.readInt();
                    }
                }
                mid = (start + end) / 2;
                randomAccessFile.seek(seekPosition + (Short.BYTES * mid));
                address = randomAccessFile.readShort();
                randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + address);
                if (pageType == Page.LEAF_TABLE_PAGE) {
                    randomAccessFile.readShort();
                    rowId = randomAccessFile.readInt();
                    if (rowId == key) return mid;
                    if (rowId > key) {
                        end = mid - 1;
                    } else {
                        start = mid + 1;
                    }
                } else if (pageType == Page.INTERIOR_TABLE_PAGE) {
                    pageNumber = randomAccessFile.readInt();
                    rowId = randomAccessFile.readInt();
                    if (rowId > key) {
                        end = mid - 1;
                    } else {
                        start = mid + 1;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private Page readPageHeader(RandomAccessFile randomAccessFile, int pageNumber) throws InternalException {
        try {
            Page page;
            randomAccessFile.seek(Page.PAGE_SIZE * pageNumber);
            byte pageType = randomAccessFile.readByte();
            if (pageType == Page.INTERIOR_TABLE_PAGE) {
                page = new Page<RecordPointer>();
            } else {
                page = new Page<DataRecord>();
            }
            page.setPageType(pageType);
            page.setPageNumber(pageNumber);
            page.setNumberOfCells(randomAccessFile.readByte());
            page.setStartingAddress(randomAccessFile.readShort());
            page.setRightNodeAddress(randomAccessFile.readInt());
            for (byte i = 0; i < page.getNumberOfCells(); i++) {
                page.getRecordAddressList().add(randomAccessFile.readShort());
            }
            return page;
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private boolean writePageHeader(RandomAccessFile randomAccessFile, Page page) throws InternalException {
        try {
            randomAccessFile.seek(page.getPageNumber() * Page.PAGE_SIZE);
            randomAccessFile.writeByte(page.getPageType());
            randomAccessFile.writeByte(page.getNumberOfCells());
            randomAccessFile.writeShort(page.getStartingAddress());
            randomAccessFile.writeInt(page.getRightNodeAddress());
            for (Object offset : page.getRecordAddressList()) {
                randomAccessFile.writeShort((short) offset);
            }
            return true;
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, DataRecord record) throws InternalException {
        try {
            randomAccessFile.seek((record.getLocationOfPage() * Page.PAGE_SIZE) + record.getOffsetVal());
            randomAccessFile.writeShort(record.getSizeOf());
            randomAccessFile.writeInt(record.getRowId());
            randomAccessFile.writeByte((byte) record.getValuesOfColumns().size());
            randomAccessFile.write(record.getSerialTypeCodes());
            for (Object object : record.getValuesOfColumns()) {
                switch (Errors.resolveClass(object)) {
                    case DatabaseDefinedConstants.TINYINT:
                        randomAccessFile.writeByte(((TinyInt_DT) object).getValue());
                        break;

                    case DatabaseDefinedConstants.SMALLINT:
                        randomAccessFile.writeShort(((SmallInt_DT) object).getValue());
                        break;

                    case DatabaseDefinedConstants.INT:
                        randomAccessFile.writeInt(((Int_DT) object).getValue());
                        break;

                    case DatabaseDefinedConstants.BIGINT:
                        randomAccessFile.writeLong(((BigInt_DT) object).getValue());
                        break;

                    case DatabaseDefinedConstants.REAL:
                        randomAccessFile.writeFloat(((Real_DT) object).getValue());
                        break;

                    case DatabaseDefinedConstants.DOUBLE:
                        randomAccessFile.writeDouble(((Double_DT) object).getValue());
                        break;

                    case DatabaseDefinedConstants.DATE:
                        randomAccessFile.writeLong(((Date_DT) object).getValue());
                        break;

                    case DatabaseDefinedConstants.DATETIME:
                        randomAccessFile.writeLong(((DateTime_DT) object).getValue());
                        break;

                    case DatabaseDefinedConstants.TEXT:
                        if (((Text_DT) object).getValue() != null)
                            randomAccessFile.writeBytes(((Text_DT) object).getValue());
                        break;

                    default:
                        break;
                }
            }
        } catch (ClassCastException e) {
            throw new InternalException(InternalException.INVALID_DATATYPE_EXCEPTION);
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return true;
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, RecordPointer record) throws InternalException {
        try {
            randomAccessFile.seek((record.getPageNumber() * Page.PAGE_SIZE) + record.getOffsetValue());
            randomAccessFile.writeInt(record.getPageNumber_Left());
            randomAccessFile.writeInt(record.getKeyValue());
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return true;
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, InternalCondition condition, boolean getOne) throws InternalException {
        return findRecord(databaseName, tableName, condition,null, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, InternalCondition condition, List<Byte> selectionColumnIndexList, boolean getOne) throws InternalException {
        List<InternalCondition> conditionList = new ArrayList<>();
        if(condition != null)
            conditionList.add(condition);
        return findRecord(databaseName, tableName, conditionList, selectionColumnIndexList, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<InternalCondition> conditionList, boolean getOne) throws InternalException {
        return findRecord(databaseName, tableName, conditionList, null, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<InternalCondition> conditionList, List<Byte> selectionColumnIndexList, boolean getOne) throws InternalException {
        try {
            File file = new File(Errors.getDatabasePath(databaseName) + "/" + tableName + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                if (conditionList != null) {
                    Page page = getFirstLeafPage(file);
                    DataRecord record;
                    List<DataRecord> matchRecords = new ArrayList<>();
                    boolean isMatch = false;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Object offset : page.getRecordAddressList()) {
                            isMatch = true;
                            record = readDataRecord(randomAccessFile, page.getPageNumber(), (short) offset);
                            for(int i = 0; i < conditionList.size(); i++) {
                                isMatch = false;
                                columnIndex = conditionList.get(i).getIndex();
                                value = conditionList.get(i).getValue();
                                condition = conditionList.get(i).getConditionType();
                                if (record != null && record.getValuesOfColumns().size() > columnIndex) {
                                    Object object = record.getValuesOfColumns().get(columnIndex);
                                    try {
                                        isMatch = compare(object, value, condition);
                                    }
                                    catch (InternalException e) {
                                        randomAccessFile.close();
                                        throw e;
                                    }
                                    catch (Exception e) {
                                        randomAccessFile.close();
                                        throw new InternalException(InternalException.GENERIC_EXCEPTION);
                                    }
                                    if(!isMatch) break;
                                }
                            }

                            if(isMatch) {
                                DataRecord matchedRecord = record;
                                if(selectionColumnIndexList != null) {
                                    matchedRecord = new DataRecord();
                                    matchedRecord.setRowId(record.getRowId());
                                    matchedRecord.setLocationOfPage(record.getLocationOfPage());
                                    matchedRecord.setOffsetVal(record.getOffsetVal());
                                    for (Byte index : selectionColumnIndexList) {
                                        matchedRecord.getValuesOfColumns().add(record.getValuesOfColumns().get(index));
                                    }
                                }
                                matchRecords.add(matchedRecord);
                                if(getOne) {
                                    randomAccessFile.close();
                                    return matchRecords;
                                }
                            }
                        }
                        if (page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return matchRecords;
                }
            } else {
                Errors.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
                return null;
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return null;
    }

    public int updateRecord(String databaseName, String tableName, InternalCondition condition, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(condition);
        return updateRecord(databaseName, tableName, conditions, updateColumnIndexList, updateColumnValueList, isIncrement);
    }

    public int updateRecord(String databaseName, String tableName, List<InternalCondition> conditions, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) throws InternalException {
        int updateRecordCount = 0;
        try {
            if (conditions == null || updateColumnIndexList == null
                    || updateColumnValueList == null)
                return updateRecordCount;
            if (updateColumnIndexList.size() != updateColumnValueList.size())
                return updateRecordCount;
            File file = new File(Errors.getDatabasePath(databaseName) + "/" + tableName + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                List<DataRecord> records = findRecord(databaseName, tableName, conditions, false);
                if (records != null) {
                    if (records.size() > 0) {
                        byte index;
                        Object object;
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                        for (DataRecord record : records) {
                            for (int i = 0; i < updateColumnIndexList.size(); i++) {
                                index = updateColumnIndexList.get(i);
                                object = updateColumnValueList.get(i);
                                if (isIncrement) {
                                    record.getValuesOfColumns().set(index, increment((Num_DT) record.getValuesOfColumns().get(index), (Num_DT) object));
                                } else {
                                    record.getValuesOfColumns().set(index, object);
                                }
                            }
                            this.writeRecord(randomAccessFile, record);
                            updateRecordCount++;
                        }
                        randomAccessFile.close();
                        return updateRecordCount;
                    }
                }
            } else {
                Errors.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return updateRecordCount;
    }

    private <T> Num_DT<T> increment(Num_DT<T> object1, Num_DT<T> object2) {
        object1.increment(object2.getValue());
        return object1;
    }

    public Page<DataRecord> getLastRecordAndPage(String databaseName, String tableName) throws InternalException {
        try {
            File file = new File(Errors.getDatabasePath(databaseName) + "/" + tableName + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                Page<DataRecord> page = getRightmostLeafPage(file);
                if (page.getNumberOfCells() > 0) {
                    randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + Page.getHeaderFixedLength() + ((page.getNumberOfCells() - 1) * Short.BYTES));
                    short address = randomAccessFile.readShort();
                    DataRecord record = readDataRecord(randomAccessFile, page.getPageNumber(), address);
                    if (record != null)
                        page.getPageRecords().add(record);
                }
                randomAccessFile.close();
                return page;
            } else {
                Errors.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
                return null;
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private Page getRightmostLeafPage(File file) throws InternalException {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE && page.getRightNodeAddress() != Page.RIGHTMOST_PAGE) {
                page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
            }
            randomAccessFile.close();
            return page;
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private Page getFirstLeafPage(File file) throws InternalException {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                if (page.getNumberOfCells() == 0) return null;
                randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + ((short) page.getRecordAddressList().get(0)));
                page = readPageHeader(randomAccessFile, randomAccessFile.readInt());
            }
            randomAccessFile.close();
            return page;
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    public int deleteRecord(String databaseName, String tableName, List<InternalCondition> conditions) throws InternalException {
        int deletedRecordCount = 0;
        try {
            File file = new File(Errors.getDatabasePath(databaseName) + "/" + tableName + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                if(conditions != null) {
                    Page page = getFirstLeafPage(file);
                    DataRecord record;
                    boolean isMatch;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Short offset : new ArrayList<Short>(page.getRecordAddressList())) {
                            isMatch = true;
                            record = readDataRecord(randomAccessFile, page.getPageNumber(), offset);
                            for(int i = 0; i < conditions.size(); i++) {
                                isMatch = false;
                                columnIndex = conditions.get(i).getIndex();
                                value = conditions.get(i).getValue();
                                condition = conditions.get(i).getConditionType();
                                if (record != null && record.getValuesOfColumns().size() > columnIndex) {
                                    Object object = record.getValuesOfColumns().get(columnIndex);
                                    try {
                                        isMatch = compare(object, value, condition);
                                    }
                                    catch (InternalException e) {
                                        randomAccessFile.close();
                                        throw e;
                                    }
                                    catch (Exception e) {
                                        randomAccessFile.close();
                                        throw new InternalException(InternalException.GENERIC_EXCEPTION);
                                    }

                                    if(!isMatch) break;
                                }
                            }
                            if(isMatch) {
                                page.setNumberOfCells((byte) (page.getNumberOfCells() - 1));
                                page.getRecordAddressList().remove(offset);
                                if(page.getNumberOfCells() == 0) {
                                    page.setStartingAddress((short) (page.getBaseAddress() + Page.PAGE_SIZE - 1));
                                }
                                this.writePageHeader(randomAccessFile, page);
                                DatabaseUtility.decrementTableRowCount(databaseName, tableName);
                                deletedRecordCount++;
                            }
                        }
                        if(page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return deletedRecordCount;
                }
            }
            else {
                Errors.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
                return deletedRecordCount;
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (ClassCastException e) {
            throw new InternalException(InternalException.INVALID_DATATYPE_EXCEPTION);
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return deletedRecordCount;
    }

    private boolean compare(Object object1, Object object2, short condition) throws InternalException {
        boolean isMatch = false;
        if(((DataType) object1).isNull()) isMatch = false;
        else {
            switch (Errors.resolveClass(object1)) {
                case DatabaseDefinedConstants.TINYINT:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.TINYINT:
                            isMatch = ((TinyInt_DT) object1).compare((TinyInt_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.SMALLINT:
                            isMatch = ((TinyInt_DT) object1).compare((SmallInt_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.INT:
                            isMatch = ((TinyInt_DT) object1).compare((Int_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.BIGINT:
                            isMatch = ((TinyInt_DT) object1).compare((BigInt_DT) object2, condition);
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                    }
                    break;

                case DatabaseDefinedConstants.SMALLINT:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.TINYINT:
                            isMatch = ((SmallInt_DT) object1).compare((TinyInt_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.SMALLINT:
                            isMatch = ((SmallInt_DT) object1).compare((SmallInt_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.INT:
                            isMatch = ((SmallInt_DT) object1).compare((Int_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.BIGINT:
                            isMatch = ((SmallInt_DT) object1).compare((BigInt_DT) object2, condition);
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                    }
                    break;

                case DatabaseDefinedConstants.INT:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.TINYINT:
                            isMatch = ((Int_DT) object1).compare((TinyInt_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.SMALLINT:
                            isMatch = ((Int_DT) object1).compare((SmallInt_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.INT:
                            isMatch = ((Int_DT) object1).compare((Int_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.BIGINT:
                            isMatch = ((Int_DT) object1).compare((BigInt_DT) object2, condition);
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                    }
                    break;

                case DatabaseDefinedConstants.BIGINT:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.TINYINT:
                            isMatch = ((BigInt_DT) object1).compare((TinyInt_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.SMALLINT:
                            isMatch = ((BigInt_DT) object1).compare((SmallInt_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.INT:
                            isMatch = ((BigInt_DT) object1).compare((Int_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.BIGINT:
                            isMatch = ((BigInt_DT) object1).compare((BigInt_DT) object2, condition);
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                    }
                    break;

                case DatabaseDefinedConstants.REAL:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.REAL:
                            isMatch = ((Real_DT) object1).compare((Real_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.DOUBLE:
                            isMatch = ((Real_DT) object1).compare((Double_DT) object2, condition);
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Decimal Number");
                    }
                    break;

                case DatabaseDefinedConstants.DOUBLE:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.REAL:
                            isMatch = ((Double_DT) object1).compare((Real_DT) object2, condition);
                            break;

                        case DatabaseDefinedConstants.DOUBLE:
                            isMatch = ((Double_DT) object1).compare((Double_DT) object2, condition);
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Decimal Number");
                    }
                    break;

                case DatabaseDefinedConstants.DATE:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.DATE:
                            isMatch = ((Date_DT) object1).compare((Date_DT) object2, condition);
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Date");
                    }
                    break;

                case DatabaseDefinedConstants.DATETIME:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.DATETIME:
                            isMatch = ((DateTime_DT) object1).compare((DateTime_DT) object2, condition);
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Datetime");
                    }
                    break;

                case DatabaseDefinedConstants.TEXT:
                    switch (Errors.resolveClass(object2)) {
                        case DatabaseDefinedConstants.TEXT:
                            if (((Text_DT) object1).getValue() != null) {
                                if (condition != InternalCondition.EQUALS) {
                                    throw new InternalException(InternalException.INVALID_CONDITION_EXCEPTION, "= is");
                                } else
                                    isMatch = ((Text_DT) object1).getValue().equalsIgnoreCase(((Text_DT) object2).getValue());
                            }
                            break;

                        default:
                            throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "String");
                    }
                    break;
            }
        }
        return isMatch;
    }

    private DataRecord readDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address) throws InternalException {
        try {
            if (pageNumber >= 0 && address >= 0) {
                DataRecord record = new DataRecord();
                record.setLocationOfPage(pageNumber);
                record.setOffsetVal(address);
                randomAccessFile.seek((Page.PAGE_SIZE * pageNumber) + address);
                record.setSizeOf(randomAccessFile.readShort());
                record.setRowId(randomAccessFile.readInt());
                byte numberOfColumns = randomAccessFile.readByte();
                byte[] serialTypeCodes = new byte[numberOfColumns];
                for (byte i = 0; i < numberOfColumns; i++) {
                    serialTypeCodes[i] = randomAccessFile.readByte();
                }
                Object object;
                for (byte i = 0; i < numberOfColumns; i++) {
                    switch (serialTypeCodes[i]) {
                        //case TinyInt_DT.nullSerialCode is overridden with Text_DT

                        case DatabaseDefinedConstants.ONE_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new Text_DT(null);
                            break;

                        case DatabaseDefinedConstants.TWO_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new SmallInt_DT(randomAccessFile.readShort(), true);
                            break;

                        case DatabaseDefinedConstants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new Real_DT(randomAccessFile.readFloat(), true);
                            break;

                        case DatabaseDefinedConstants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new Double_DT(randomAccessFile.readDouble(), true);
                            break;

                        case DatabaseDefinedConstants.TINY_INT_SERIAL_TYPE_CODE:
                            object = new TinyInt_DT(randomAccessFile.readByte());
                            break;

                        case DatabaseDefinedConstants.SMALL_INT_SERIAL_TYPE_CODE:
                            object = new SmallInt_DT(randomAccessFile.readShort());
                            break;

                        case DatabaseDefinedConstants.INT_SERIAL_TYPE_CODE:
                            object = new Int_DT(randomAccessFile.readInt());
                            break;

                        case DatabaseDefinedConstants.BIG_INT_SERIAL_TYPE_CODE:
                            object = new BigInt_DT(randomAccessFile.readLong());
                            break;

                        case DatabaseDefinedConstants.REAL_SERIAL_TYPE_CODE:
                            object = new Real_DT(randomAccessFile.readFloat());
                            break;

                        case DatabaseDefinedConstants.DOUBLE_SERIAL_TYPE_CODE:
                            object = new Double_DT(randomAccessFile.readDouble());
                            break;

                        case DatabaseDefinedConstants.DATE_SERIAL_TYPE_CODE:
                            object = new Date_DT(randomAccessFile.readLong());
                            break;

                        case DatabaseDefinedConstants.DATE_TIME_SERIAL_TYPE_CODE:
                            object = new DateTime_DT(randomAccessFile.readLong());
                            break;

                        case DatabaseDefinedConstants.TEXT_SERIAL_TYPE_CODE:
                            object = new Text_DT("");
                            break;

                        default:
                            if (serialTypeCodes[i] > DatabaseDefinedConstants.TEXT_SERIAL_TYPE_CODE) {
                                byte length = (byte) (serialTypeCodes[i] - DatabaseDefinedConstants.TEXT_SERIAL_TYPE_CODE);
                                char[] text = new char[length];
                                for (byte k = 0; k < length; k++) {
                                    text[k] = (char) randomAccessFile.readByte();
                                }
                                object = new Text_DT(new String(text));
                            } else
                                object = null;
                            break;
                    }
                    record.getValuesOfColumns().add(object);
                }
                return record;
            }
        } catch (ClassCastException e) {
            throw new InternalException(InternalException.INVALID_DATATYPE_EXCEPTION);
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return null;
    }

}
