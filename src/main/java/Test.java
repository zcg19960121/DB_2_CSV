import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.List;

public class Test {

    private static void makeCSV(String CSV_File_Path, String FileName, ResultSet rs) throws IOException, SQLException {
        CSVWriterBuilder a = new CSVWriterBuilder(new FileWriter(CSV_File_Path + FileName + ".csv", false)).withSeparator(',').withQuoteChar('\u0000');
        ICSVWriter writer = a.build();
        writer.writeAll(rs, false);
        writer.close();
    }

    private static List<String> getStringContent(String SqlBatch_Location) throws IOException {
        List<String> list = FileUtils.readLines(new File(SqlBatch_Location), "UTF-8");
        return list;
    }

    private static String[][] getSQLString(List<String> list) {
        int length = list.size();
        String[][] strArray = new String[length][3];
        String[] tmp = null;
        int x = 0;
        for (String str : list) {
            tmp = str.split("\\^");
            strArray[x][0] = tmp[0];
            strArray[x][1] = tmp[1];
            strArray[x][2] = tmp[2];
            x++;
        }

        return strArray;
    }

    private static String[][] getDBConfigString(List<String> list) {
        int length = list.size();
        String[][] strArray = new String[length][5];
        String[] tmp = null;
        int x = 0;
        for (String str : list) {
            tmp = str.split("\\^");
            strArray[x][0] = tmp[0];
            strArray[x][1] = tmp[1];
            strArray[x][2] = tmp[2];
            strArray[x][3] = tmp[3];
            strArray[x][4] = tmp[4];
            x++;
        }

        return strArray;
    }

    private static Connection getDBConnection(String driverName, String dbURL, String userName, String userPwd) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        return conn;
    }


    public static void main(String[] args) throws IOException, SQLException {
        if (args.length == 3){
//            String DBConfig_Location = "C:\\Users\\GG\\Desktop\\HSBC_JAVA\\DB.config";
//            String SqlBatch_Location = "C:\\Users\\GG\\Desktop\\HSBC_JAVA\\sqlBatch";
//            String CSV_Location = "C:\\Users\\GG\\Desktop\\HSBC_JAVA\\";
            String DBConfig_Location = args[0];
            String SqlBatch_Location = args[1];
            String CSV_Location = args[2];
            List<String> sqlList = getStringContent(SqlBatch_Location);
            List<String> dbConfigList = getStringContent(DBConfig_Location);
            String[][] sqlArray = getSQLString(sqlList);
            String[][] dbConfigArray = getDBConfigString(dbConfigList);
            Connection conn = null;
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            for (String[] db : dbConfigArray) {
                try {
                    conn = getDBConnection(db[1], db[2], db[3], db[4]);
                } catch (Exception e) {
                    conn.close();
                    e.printStackTrace();
                }
                for (String[] sql : sqlArray) {
                    if (db[0] != null && db[0].equals(sql[0])) {
                        try {
                            pstmt = conn.prepareStatement(sql[2]);
                            rs = pstmt.executeQuery();
                            makeCSV(CSV_Location, sql[1], rs);
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            rs.close();
                            pstmt.close();
                        }
                    }
                }
                conn.close();
            }
        }


    }
}
