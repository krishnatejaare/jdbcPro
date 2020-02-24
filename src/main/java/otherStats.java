import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class otherStats {

  public static String Percentile(List<Long> latencies, double Percentile)
  {
    Collections.sort(latencies);
    int Index = (int)Math.ceil(((double)Percentile / (double)100) * (double)latencies.size());
    return latencies.get(Index-1).toString();
  }

  public static void main(String[] args) {

    String url = "jdbc:mysql://localhost:3306/ivedadb";
    String user = "iv_readonly";
    String password = "irpt_pwd123";
    File file = new File("/Users/krishnateja/Downloads/jdbcPro/src/main/resources/percentile.txt");
    File pile = new File("/Users/krishnateja/Downloads/jdbcPro/src/main/resources/halfresult.txt");
    BufferedWriter bf = null;
    BufferedWriter bf2 = null;
    try {
      bf = new BufferedWriter(new FileWriter(file, true));
      bf2 = new BufferedWriter(new FileWriter(pile, true));
      String[] tables = {"'%bl\\_qs\\_notfn\\_events%'", "'%al\\_mmcs\\_move\\_complete%'",
          "'%bl\\_mmcs\\_accounts\\_gcbd%'", "'%cl\\_icloud\\_active\\_usage\\_monthly%'",
          "'%sl\\_qs\\_active\\_storage\\_subscribers%'", "'%sl\\_mme\\_datastore%'",
          "'%sl\\_accounts%'",
          "'%bl\\_psm\\_join\\_by\\_episode\\_title%'", "'%bl\\_qs\\_success\\_events%'",
          "'%bl\\_ubq\\_document%'", "'%sl\\_mme\\_account%'",
          "'%cl\\_qs\\_ever\\_storage\\_subscriptions%'", "'%bl\\_gamecenter\\_matchmaking%'","'%cl\\_qs\\_morocco\\_events%'","'%sl\\_mme\\_assignment%'","'%sl\\_qs\\_current\\_user\\_quota%'","'%bl\\_gamecenter\\_session\\_events%'","'%al\\_apns\\_device\\_presence%'","'%cl\\_icloud\\_active\\_usage\\_monthly\\_new\\_fm%'","'%bl\\_gamecenter\\_challenge\\_events%'"};
      for (String tablename : tables) {
        String query =
            "select query, start_time_ts, end_time_ts, ROUND((END_TIME_TS - START_TIME_TS) /(1000)) as time_taken,FROM_UNIXTIME(START_TIME_TS / 1000, '%Y-%m-%d') AS execution_date, query_id from ivedadb.IV_QUERY_LOG_STATS a LEFT OUTER JOIN ivedadb.IV_SELF_SERVE_USER b ON (a.user_id = b.name) where START_TIME_TS >= Unix_timestamp(STR_TO_DATE('2019-12-01', '%Y-%m-%d'))*1000 and STATUS!='ERROR' and query like "
                + tablename;
        List<Long> latencies = new ArrayList<Long>();
        try {
          Connection con = DriverManager.getConnection(url, user, password);
          Statement st = con.createStatement();
          ResultSet rs = st.executeQuery(query);
          // the below commented code is for calculating percentiles
//          while (rs.next()) {
//            if (Long.parseLong(rs.getString(3)) != 0) {
//              latencies.add(Long.parseLong(rs.getString(4)));
//            }
//          }
//          try {
//            bf.write(
//                tablename.replace("%","").replace("\\","").replace("'","") + "," + Percentile(latencies, 50) + "," + Percentile(latencies, 75) + ","
//                    + Percentile(latencies, 90) + "," + Percentile(latencies, 99));
//            bf.newLine();
//          } catch (Exception e) {}
          List<Transaction> finalTransactions = new ArrayList<>();
          try {
            while(rs.next()){
              String tempQuery = rs.getString(1);
              List<String>splitQuery = Arrays.asList(tempQuery.replaceAll("date ","date").replaceAll("= ","=").replaceAll(" =","=").replaceAll(" >",">").replaceAll("> ",">").replaceAll(" <","<").replaceAll("< ","<").toLowerCase().replaceAll(" between","between").replaceAll("between ","between").split(" "));
              String fromDate = null;
              String toDate = null;
              for(int i=0; i<splitQuery.size();i++) {
                if(splitQuery.get(i).equals(tablename.replace("%","").replace("\\","").replace("'",""))){
                  for(int j=i+1;j<splitQuery.size();j++) {
                    if(splitQuery.get(j).contains("date") && !splitQuery.get(j).contains("between")) {
                      if(splitQuery.get(j).contains("date=")){
                        fromDate = splitQuery.get(j);
                        break;
                      } else if(splitQuery.get(j).contains("date>=") || splitQuery.get(j).contains("date>")){
                        fromDate = splitQuery.get(j);
                        for(int k=j+1;k<splitQuery.size();k++){
                          if(splitQuery.get(k).contains("date<=") || splitQuery.get(k).contains("date<")) {
                            toDate = splitQuery.get(k);
                            break;
                          }
                        }
                        break;
                      }
                    } else if(splitQuery.get(j).contains("date") && splitQuery.get(j).contains("between")) {
                      fromDate = splitQuery.get(j);
                      toDate = splitQuery.get(j+2);
                      break;
                    }
                  }
                  break;
                }
              }
              Transaction t = new Transaction();
              t.setQuery(rs.getString(1));
              t.setStartTime(rs.getString(2));
              t.setEndTime(rs.getString(3));
              t.setTimeTaken(rs.getString(4));
              t.setExecutionDate(rs.getString(5));
              t.setQueryId(rs.getString(6));
              t.setFromDate(fromDate);
              t.setToDate(toDate);
              t.setTableName(tablename.replace("%","").replace("\\","").replace("'",""));
              finalTransactions.add(t);
              try {
                bf2.write(t.getTableName()+"$$"+t.getQueryId()+"$$"+t.getStartTime()+"$$"+t.getEndTime()+"$$"+t.getTimeTaken()+"$$"+t.getExecutionDate()+"$$"+t.getFromDate()+"$$"+t.getToDate());
                bf2.newLine();
              } catch (Exception e) {}
            }
          } catch (Exception e){}


        } catch (Exception ex) {
          System.out.println(ex);
        }
      }
    } catch(Exception e){

      } finally{
      try{
        //bf.close();
        bf2.close();
      }catch(Exception e){}
    }
  }
}
