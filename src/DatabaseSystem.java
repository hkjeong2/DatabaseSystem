import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class DatabaseSystem implements API{

    private Connection conn;
    private Statement stmt;
    private String url = "";
    private String user = "";
    private String password = "";

    DatabaseSystem(){
        try{
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
        }
        catch (SQLException sqle){
            System.out.println("SQLException : " + sqle);
        }
    }

    @Override
    public void createTable() {
        if (!isTableExisting()){
            String sql = "CREATE TABLE stock (" +
                    "id INT NOT NULL AUTO_INCREMENT," +
                    "name VARCHAR(20) NOT NULL," +
                    "market VARCHAR(20) NOT NULL," +
                    "sector VARCHAR(20) NOT NULL," +
                    "price INT NOT NULL," +
                    "volume INT NOT NULL," +
                    "PRIMARY KEY (id)" +
                    ");";
            try {
                stmt.executeUpdate(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            System.out.println("table 생성!");
        }
        else{
            System.out.println("table이 이미 존재합니다!");
        }
    }

    @Override
    public void insertRecord() {
        String[] market = {"코스피","코스닥"};
        String[] sector = {"서비스", "전기전자", "게임", "화학", "기계", "철강금속"};
        int[] price = {10, 100, 1000};
        int[] volume = {10, 100, 1000, 10000, 100000};

        Random random = new Random();

        for (int i = 0; i < 10000; i++){
            String query = "INSERT INTO stock (name, market, sector, price, volume) VALUES (CONCAT('종목', LAST_INSERT_ID()), ?, ?, ?, ?)";

            try {
                PreparedStatement stmt = conn.prepareStatement(query);

                stmt.setString(1, market[random.nextInt(market.length)]);
                stmt.setString(2, sector[random.nextInt(sector.length)]);
                stmt.setInt(3, (random.nextInt(1500)+1) * price[random.nextInt(price.length)]);
                stmt.setInt(4, (random.nextInt(1500)+1) * volume[random.nextInt(volume.length)]);
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("레코드 대량 생성!");
    }

    @Override
    public void insertRecord(String market, String sector, int price, int volume) {

        String query = "INSERT INTO stock (name, market, sector, price, volume) VALUES (CONCAT('종목', LAST_INSERT_ID()), ?, ?, ?, ?)";

        try {
            PreparedStatement stmt = conn.prepareStatement(query);

            stmt.setString(1, market);
            stmt.setString(2, sector);
            stmt.setInt(3, price);
            stmt.setInt(4, volume);
            stmt.executeUpdate();
            System.out.println("레코드 개별 생성!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createBitmapIndex() {
        String query = "SELECT * FROM stock";
        HashMap<String, String> bitMap = new HashMap<>();

        String[] bitMapName = {"kospiBit", "kosdaqBit", "serviceBit", "electronicBit", "gameBit", "chemicalBit", "machineBit", "metalBit", "price1Bit", "price2Bit", "price3Bit", "price4Bit", "price5Bit", "price6Bit", "price7Bit", "volume1Bit", "volume2Bit", "volume3Bit", "volume4Bit", "volume5Bit"};
        for (String value : bitMapName)
            bitMap.put(value, "");

        try (
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String market = rs.getString("market");
                String sector = rs.getString("sector");
                int price = rs.getInt("price");
                int volume = rs.getInt("volume");

                for (String value : bitMapName) {
                    String s = bitMap.get(value) + "0";
                    bitMap.put(value, s);
                }

                char[] chars;

                switch (market){
                    case "코스피" :
                        chars = bitMap.get("kospiBit").toCharArray();
                        chars[chars.length-1] = '1';
                        bitMap.put("kospiBit", String.valueOf(chars));
                        break;
                    case "코스닥" :
                        chars = bitMap.get("kosdaqBit").toCharArray();
                        chars[chars.length-1] = '1';
                        bitMap.put("kosdaqBit", String.valueOf(chars));
                        break;
                    default:
                        System.out.println("invalid input");
                        break;
                }

                switch (sector){
                    case "서비스":
                        chars = bitMap.get("serviceBit").toCharArray();
                        chars[chars.length-1] = '1';
                        bitMap.put("serviceBit", String.valueOf(chars));
                        break;
                    case "전기전자":
                        chars = bitMap.get("electronicBit").toCharArray();
                        chars[chars.length-1] = '1';
                        bitMap.put("electronicBit", String.valueOf(chars));
                        break;
                    case "게임":
                        chars = bitMap.get("gameBit").toCharArray();
                        chars[chars.length-1] = '1';
                        bitMap.put("gameBit", String.valueOf(chars));
                        break;
                    case "화학":
                        chars = bitMap.get("chemicalBit").toCharArray();
                        chars[chars.length-1] = '1';
                        bitMap.put("chemicalBit", String.valueOf(chars));
                        break;
                    case "기계":
                        chars = bitMap.get("machineBit").toCharArray();
                        chars[chars.length-1] = '1';
                        bitMap.put("machineBit", String.valueOf(chars));
                        break;
                    case "철강금속":
                        chars = bitMap.get("metalBit").toCharArray();
                        chars[chars.length-1] = '1';
                        bitMap.put("metalBit", String.valueOf(chars));
                        break;
                    default:
                        System.out.println("invalid input");
                        break;
                }

                if (price >= 1 && price <= 4999){
                    chars = bitMap.get("price1Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("price1Bit", String.valueOf(chars));
                }
                else if (price >= 5000 && price <= 9999){
                    chars = bitMap.get("price2Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("price2Bit", String.valueOf(chars));
                }
                else if (price >= 10000 && price <= 49999){
                    chars = bitMap.get("price3Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("price3Bit", String.valueOf(chars));
                }
                else if (price >= 50000 && price <= 99999){
                    chars = bitMap.get("price4Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("price4Bit", String.valueOf(chars));
                }
                else if (price >= 100000 && price <= 499999){
                    chars = bitMap.get("price5Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("price5Bit", String.valueOf(chars));
                }
                else if (price >= 500000 && price <= 999999){
                    chars = bitMap.get("price6Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("price6Bit", String.valueOf(chars));
                }
                else if (price >= 1000000){
                    chars = bitMap.get("price7Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("price7Bit", String.valueOf(chars));
                }

                if (volume >= 0 && volume <= 99999){
                    chars = bitMap.get("volume1Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("volume1Bit", String.valueOf(chars));
                }
                else if (volume >= 100000 && volume <= 999999){
                    chars = bitMap.get("volume2Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("volume2Bit", String.valueOf(chars));
                }
                else if (volume >= 1000000 && volume <= 9999999){
                    chars = bitMap.get("volume3Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("volume3Bit", String.valueOf(chars));
                }
                else if (volume >= 10000000 && volume <= 99999999){
                    chars = bitMap.get("volume4Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("volume4Bit", String.valueOf(chars));
                }
                else if (volume >= 100000000){
                    chars = bitMap.get("volume5Bit").toCharArray();
                    chars[chars.length-1] = '1';
                    bitMap.put("volume5Bit", String.valueOf(chars));
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            for (int i = 0; i < bitMap.size(); i++){
                FileOutputStream fos = new FileOutputStream(bitMapName[i] + ".txt");
                byte[] bytes = bitMap.get(bitMapName[i]).getBytes();
                for (byte aByte : bytes)
                    fos.write(aByte);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("bitmap index 생성!");
    }

    @Override
    public void searchRecord(String market, String sector, int price, int volume) {

        BitSet result = resultBit(market, sector, price, volume);
        List<Integer> resultIdx = resultIdx(result);

        for (int id : resultIdx) {
            String query = "SELECT * FROM stock WHERE id = ?";

            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setInt(1, id);
                ResultSet rs = stmt.executeQuery();

                if (rs.next()) {
                    System.out.println("종목이름: " + rs.getString("name") + "\t" +
                            "거래소: " + rs.getString("market") + "\t" +
                            "업종: " + rs.getString("sector") + "\t" +
                            "가격: " + rs.getInt("price") + "\t" +
                            "거래량: " + rs.getInt("volume"));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void countRecord(String market, String sector, int price, int volume) {

        BitSet result = resultBit(market, sector, price, volume);
        List<Integer> resultIdx = resultIdx(result);
        int count = resultIdx.size();

        System.out.println("검색하신 결과는 총 " +  count + "건 입니다.");
    }

    @Override
    public void showTableCreated() {
        if (isTableExisting()){
            System.out.println("종목 table이 존재합니다");
        }
        else{
            System.out.println("종목 table이 존재하지 않습니다");
        }
    }

    @Override
    public void showRecordInserted(int number) {

        String query = "SELECT * FROM stock ORDER BY id DESC LIMIT ?";

        try {
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setInt(1, number);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                String name = rs.getString("name");
                String market = rs.getString("market");
                String sector = rs.getString("sector");
                int price = rs.getInt("price");
                int volume = rs.getInt("volume");

                System.out.println("종목이름: " + name + ", 거래소: " + market + ", 업종: " + sector + ", 가격: " + price + ", 거래량: " + volume);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void showBitmapIndex(String fileName) {
        String bitmap = readFile(fileName);
        if (!bitmap.equals("")){
            System.out.println("해당 파일의 bitmap index 입니다");
            System.out.println(bitmap);
        }
        else{
            System.out.println("존재하지 않는 파일입니다");
        }
    }

    @Override
    public void showResultBits(String market, String sector, int price, int volume) {

        BitSet result = resultBit(market, sector, price, volume);
        String bitmapIndex = bitToString(result);

        System.out.println("질의 결과의 bitmap index 입니다.");
        System.out.println(bitmapIndex);
    }

    @Override
    public void closeConnection() {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Boolean isTableExisting(){
        DatabaseMetaData dbm;
        try {
            dbm = conn.getMetaData();
            ResultSet rs = dbm.getTables(null, null, "stock", null);
            if(rs.next()){
                return true;
            }
            else{
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String marketBit(String market){
        String marketBit = "";
        switch (market){
            case "코스피" :
                marketBit = readFile("kospiBit.txt");
                break;
            case "코스닥" :
                marketBit = readFile("kosdaqBit.txt");
                break;
            default:
                break;
        }
        return marketBit;
    }

    private String sectorBit(String sector){
        String sectorBit = "";
        switch (sector){
            case "서비스":
                sectorBit = readFile("serviceBit.txt");
                break;
            case "전기전자":
                sectorBit = readFile("electronicBit.txt");
                break;
            case "게임":
                sectorBit = readFile("gameBit.txt");
                break;
            case "화학":
                sectorBit = readFile("chemicalBit.txt");
                break;
            case "기계":
                sectorBit = readFile("machineBit.txt");
                break;
            case "철강금속":
                sectorBit = readFile("metalBit.txt");
                break;
            default:
                break;
        }
        return sectorBit;
    }

    private String priceBit(Integer price){
        String priceBit = "";
        if (price >= 1 && price <= 4999){
            priceBit = readFile("price1Bit.txt");
        }
        else if (price >= 5000 && price <= 9999){
            priceBit = readFile("price2Bit.txt");
        }
        else if (price >= 10000 && price <= 49999){
            priceBit = readFile("price3Bit.txt");
        }
        else if (price >= 50000 && price <= 99999){
            priceBit = readFile("price4Bit.txt");
        }
        else if (price >= 100000 && price <= 499999){
            priceBit = readFile("price5Bit.txt");
        }
        else if (price >= 500000 && price <= 999999){
            priceBit = readFile("price6Bit.txt");
        }
        else if (price >= 1000000){
            priceBit = readFile("price7Bit.txt");
        }
        return priceBit;
    }

    private String volumeBit(Integer volume){
        String volumeBit = "";
        if (volume >= 0 && volume <= 99999){
            volumeBit = readFile("volume1Bit.txt");
        }
        else if (volume >= 100000 && volume <= 999999){
            volumeBit = readFile("volume2Bit.txt");
        }
        else if (volume >= 1000000 && volume <= 9999999){
            volumeBit = readFile("volume3Bit.txt");
        }
        else if (volume >= 10000000 && volume <= 99999999){
            volumeBit = readFile("volume4Bit.txt");
        }
        else if (volume >= 100000000){
            volumeBit = readFile("volume5Bit.txt");
        }
        return volumeBit;
    }


    private BitSet stringToBit(String bitString) {
        BitSet bitSet = new BitSet(bitString.length());
        for (int i = 0; i < bitString.length(); i++) {
            if (bitString.charAt(i) == '1') {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    private List resultIdx(BitSet bitSet) {
        List resultIdx = new ArrayList<Integer>();
        for (int i = 0; i < bitSet.length(); i++) {
            if (bitSet.get(i)){     //bit가 1인 것들의 위치 (id) 추가
                resultIdx.add(i+1);
            }
        }
        return resultIdx;
    }

    private String bitToString(BitSet bitSet) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bitSet.length(); i++) {
            if (bitSet.get(i)){     //Bitmap Index into String
                sb.append("1");
            }
            else{
                sb.append("0");
            }
        }
        return sb.toString();
    }

    private BitSet resultBit(String market, String sector, int price, int volume){
        String marketBit, sectorBit, priceBit, volumeBit;

        marketBit = marketBit(market);
        sectorBit = sectorBit(sector);
        priceBit = priceBit(price);
        volumeBit = volumeBit(volume);

        int bitLength = Math.max(Math.max(marketBit.length(), sectorBit.length()), Math.max(priceBit.length(), volumeBit.length()));

        if (marketBit.equals("")){
            for (int i = 0; i < bitLength; i++)
                marketBit += "1";
        }
        if (sectorBit.equals("")){
            for (int i = 0; i < bitLength; i++)
                sectorBit += "1";
        }
        if (priceBit.equals("")){
            for (int i = 0; i < bitLength; i++)
                priceBit += "1";
        }
        if (volumeBit.equals("")){
            for (int i = 0; i < bitLength; i++)
                volumeBit += "1";
        }

        BitSet marketBitset = stringToBit(marketBit);
        BitSet sectorBitset = stringToBit(sectorBit);
        BitSet priceBitset = stringToBit(priceBit);
        BitSet volumeBitset = stringToBit(volumeBit);

        BitSet result = new BitSet();   //and 연산
        result.or(marketBitset);
        result.and(sectorBitset);
        result.and(priceBitset);
        result.and(volumeBitset);

        return result;
    }

    private String readFile(String fileName){
        StringBuilder bitmap = new StringBuilder();
        try {
            File file = new File(fileName);
            FileInputStream fis = new FileInputStream(file);

            int byteRead;

            while ((byteRead = fis.read()) != -1) {
                bitmap.append((char) byteRead);
            }

//            byte[] bytes = new byte[(int) file.length()];
//            fis.read(bytes);
//
//            bitmap = new String(bytes);

            fis.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return bitmap.toString();
    }

}
