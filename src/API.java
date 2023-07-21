public interface API {
    void createTable();
    void insertRecord();
    void insertRecord(String market, String sector, int price, int volume);
    void createBitmapIndex();
    void searchRecord(String market, String sector, int price, int volume);
    void countRecord(String market, String sector, int price, int volume);
    void showTableCreated();
    void showRecordInserted(int number);
    void showBitmapIndex(String fileName);
    void showResultBits(String market, String sector, int price, int volume);
    void closeConnection();
}
