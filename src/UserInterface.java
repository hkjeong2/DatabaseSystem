import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class UserInterface {
    public static void main(String[] args) {
        UserInterface UI = new UserInterface();
        UI.startMenu();
    }

    private InputStreamReader reader = new InputStreamReader(System.in);
    private BufferedReader br = new BufferedReader(reader);
    private Scanner sc = new Scanner(System.in);
    private API DBSystemAPI;

    UserInterface(){
        DBSystemAPI = new DatabaseSystem();
    }

    private void startMenu() {
        baseMenu();
    }

    private void baseMenu() {
        while (true){
            System.out.println("********** 메인 메뉴 **********");
            System.out.println("1. 기능호출\t" + "2. 기능 테스트\t" + "3. 종료");
            System.out.print("입력 : ");

            int num = sc.nextInt();
            switch (num){
                case 1:
                    functionMenu();
                    break;
                case 2:
                    testMenu();
                    break;
                case 3:
                    DBSystemAPI.closeConnection();
                    System.out.println("Program 종료 ...");
                    return;
            }
        }
    }

    private void functionMenu(){
        System.out.println("********** 기능 메뉴 **********");
        System.out.println("1. 테이블 생성     " + "2. 레코드 삽입     " + "3. bitmap index 생성     " + "4. 질의 처리");
        System.out.print("입력 : ");

        int num = sc.nextInt();

        switch (num){
            case 1:
                createTable();
                break;
            case 2:
                insertRecord();
                break;
            case 3:
                createBitmapIndex();
                break;
            case 4:
                search();
                break;
        }
    }

    private void testMenu(){
        System.out.println("********** 기능 테스트 메뉴 **********");
        System.out.println("1. 테이블 확인     " + "2. 레코드 확인     " + "3. bitmap index 확인     " + "4. 질의 처리 bit 확인");
        System.out.print("입력 : ");

        int num = sc.nextInt();

        switch (num){
            case 1:
                showTableCreated();
                break;
            case 2:
                showRecordInserted();
                break;
            case 3:
                showBitmapIndex();
                break;
            case 4:
                showResultBits();
                break;
        }
    }

    private void createTable(){
        System.out.println("주식 종목 table 생성 시도중...");
        DBSystemAPI.createTable();
    }

    private void insertRecord() {
        System.out.println("1. 대량 삽입     " + "2. 개별 삽입");
        System.out.print("입력 : ");

        int num = sc.nextInt();
        switch (num){
            case 1:
                System.out.println("데이터를 대량 생성 및 삽입합니다");
                DBSystemAPI.insertRecord();
                break;
            case 2:
                System.out.println("삽입할 데이터의 정보를 ','를 기준으로 나누어 입력해 주세요");
                System.out.print("입력 : ");
                String[] inputToken = inputResult();
                DBSystemAPI.insertRecord(inputToken[0], inputToken[1], Integer.parseInt(inputToken[2]), Integer.parseInt(inputToken[3]));
                break;
        }
    }

    private void createBitmapIndex(){
        System.out.println("Bitmap Index를 생성합니다");
        DBSystemAPI.createBitmapIndex();
    }

    private void search(){
        System.out.println("1. Multiple-key     " + "2. Count");

        String[] inputToken;
        int num = sc.nextInt();

        System.out.println("검색할 데이터의 정보를 ','를 기준으로 나누어 입력해 주세요");
        System.out.print("입력 : ");

        switch (num){
            case 1:
                inputToken = inputResult();
                DBSystemAPI.searchRecord(inputToken[0], inputToken[1], Integer.parseInt(inputToken[2]), Integer.parseInt(inputToken[3]));
                break;
            case 2:
                inputToken = inputResult();
                DBSystemAPI.countRecord(inputToken[0], inputToken[1], Integer.parseInt(inputToken[2]), Integer.parseInt(inputToken[3]));
                break;
        }
    }

    private void showTableCreated(){
        DBSystemAPI.showTableCreated();
    }

    private void showRecordInserted(){
        System.out.println("확인할 데이터의 개수를 입력해주세요");
        System.out.print("입력 : ");
        int num = sc.nextInt();
        DBSystemAPI.showRecordInserted(num);
    }

    private void showBitmapIndex(){
        System.out.println("Bitmap Index를 확인할 파일 이름을 입력해주세요");
        System.out.print("입력 : ");
        String fileName;
        try{
            fileName = br.readLine();
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
        DBSystemAPI.showBitmapIndex(fileName);
    }

    private void showResultBits(){
        System.out.println("결과를 확인할 데이터 정보를 입력해주세요");
        System.out.print("입력 : ");
        String[] inputToken = inputResult();
        DBSystemAPI.showResultBits(inputToken[0], inputToken[1], Integer.parseInt(inputToken[2]), Integer.parseInt(inputToken[3]));
    }

    private String[] inputResult(){
        String input;
        try{
            input = br.readLine();
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
        return input.split(",");
    }

}