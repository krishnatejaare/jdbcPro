import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class jdbcProgram {
  public static HashMap<String, List<String>> userQueryMapping = new HashMap<String, List<String>>();
  public static HashMap<String, Integer> tableMapping = new HashMap<String, Integer>();
  public static HashMap<String, HashMap<String,Integer>> userTableCountMapping = new HashMap<String, HashMap<String, Integer>>();
  public static void writeToFile() {
    File file = new File("/Users/krishnateja/Downloads/jdbcPro/src/main/resources/newresult.txt");
    File tableFile = new File("/Users/krishnateja/Downloads/jdbcPro/src/main/resources/newtableMap.txt");
    BufferedWriter bf = null;
    BufferedWriter bf2 = null;
    try{
      bf = new BufferedWriter( new FileWriter(file) );
      bf2 = new BufferedWriter( new FileWriter(tableFile) );
      for(Map.Entry<String, HashMap<String, Integer>> entry : userTableCountMapping.entrySet()){
        bf.write( entry.getKey() + ":");
        for(Map.Entry<String, Integer> entr : entry.getValue().entrySet()){
          bf.newLine();
          bf.write("      "+ entr.getKey() + ": " + entr.getValue());
        }
        bf.newLine();
        bf.newLine();

      }
      for(Map.Entry<String, Integer> entry : tableMapping.entrySet()){
          bf2.write(entry.getKey() + "  " + entry.getValue());
          bf2.newLine();
        }

      bf.flush();
      bf2.flush();

    }catch(IOException e){
      e.printStackTrace();
    }finally{

      try{
        bf.close();
        bf2.close();
      }catch(Exception e){}
    }
  }
  public static void main(String[] args) {

    String url = "jdbc:mysql://localhost:3306/ivedadb";
    String user = "iv_readonly";
    String password = "irpt_pwd123";

    String query = "select user_id, lower(query) from ivedadb.IV_QUERY_LOG_STATS a LEFT OUTER JOIN ivedadb.IV_SELF_SERVE_USER b ON (a.user_id = b.name) where START_TIME_TS >= Unix_timestamp(STR_TO_DATE('2019-12-01', '%Y-%m-%d'))*1000 and STATUS!='ERROR'";
    //String query = "select user_id, lower(query) FROM ivedadb.IV_QUERY_LOG_STATS a LEFT OUTER JOIN ivedadb.IV_SELF_SERVE_USER b ON (a.user_id = b.name) where b.NAME IS NULl and START_TIME_TS >= Unix_timestamp(STR_TO_DATE('2019-12-01', '%Y-%m-%d'))*1000 and STATUS!='ERROR'";


    try {
      Connection con = DriverManager.getConnection(url, user, password);
      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery(query);
      while(rs.next()) {
          String userName = rs.getString(1);
          String userQuery = rs.getString(2);
          if(userQueryMapping.containsKey(userName)) {
            List<String>tmp =  userQueryMapping.get(userName);
            tmp.add(userQuery);
            userQueryMapping.put(userName,tmp);
        } else {
            List<String>tmp = new ArrayList<String>();
            tmp.add(userQuery);
            userQueryMapping.put(userName, tmp);
        }
      }
      for (String userName:userQueryMapping.keySet()) {
        HashMap<String, Integer> tableAndCount = new HashMap<String, Integer>();
        List<String> queryListOfUser = userQueryMapping.get(userName);
        for(String que:queryListOfUser) {
          List<String>splitQuery = Arrays.asList(que.split(" "));
          for(String tmp:splitQuery) {
            tmp =tmp.replace("\n","").replace(";","").replace(",","");
            if ((tmp.startsWith("bl_") || tmp.startsWith("al_") || tmp.startsWith("sl_") || tmp.startsWith("cl_")) && !tmp.contains("spark") && !tmp.contains(")") && !tmp.contains("(")) {
              if (tableAndCount.containsKey(tmp)) {
                tableAndCount.put(tmp, tableAndCount.get(tmp) + 1);
              } else {
                tableAndCount.put(tmp, 1);
              }
              if (tableMapping.containsKey(tmp)) {
                tableMapping.put(tmp,tableMapping.get(tmp)+1);
              } else {
                tableMapping.put(tmp, 1);
              }
            }
          }
        }
        userTableCountMapping.put(userName, tableAndCount);
      }
      writeToFile();

    } catch (Exception ex) {
      System.out.println(ex);
    }
  }
}
