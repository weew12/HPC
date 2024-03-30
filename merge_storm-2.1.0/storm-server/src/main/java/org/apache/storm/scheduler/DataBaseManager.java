package org.apache.storm.scheduler;

import java.sql.*;
import java.util.Map;


public class DataBaseManager {
    public static Connection conn = null;
    //public  ResultSet recordSet=null;


        public static void connectSql() {
         conn = null;
        try {
            // db parameters
            String url = "jdbc:sqlite:/home/ali/work/apache-storm-2.1.0/MachineLearningDB";
            // create a connection to the database
            conn = DriverManager.getConnection(url);

            System.out.println("Connection to SQLite has been established.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (conn == null) {

                    conn.close();
                  //  int a=0;
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }



    public static void insertTable( ElasticityComponentInfo elasticityComponentInfo , Map<Double, Integer>  migrationCost ) {

        System.out.println("....inserting.");
      //  String sql = "INSERT INTO Info(Id,Capacity) VALUES(?,?)";
        //String sql = "INSERT INTO MachineLearningModel(Id,Capacity) VALUES(?,?)";
        String sql = "INSERT INTO MachineLearningModel"+
                "(topologyID,component,sourceSuperviorID,targetSuperviorID,sourceCapacity, "+
                "targetWorkerNodeCpuLoad,sourceWorkerNodeCpuLoad,migrationDuration,cost,proper,inputRateTopology)"+
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)";





        try{
            //Connection conn = this.conn;
            PreparedStatement pstmt = conn.prepareStatement(sql);

           // String topologyID ="";
            String topologyName ="";
            String component="" ;
           // String sourceSuperviorID="" ;
           // String targetSuperviorID ="";
            String sourceSuperviorName="" ;
            String targetSuperviorName ="";
            Double sourceComponentCPUUsage =1.0; ;
            Double targetWorkerNodeCpuLoad =1.0 ;
            Double sourceWorkerNodeCpuLoad =1.0 ;
            Double  sourceTopologyCompleteLatencyBefore=0.0;
            Double  sourceTopologyCompleteLatencyAfter=0.0;
            Double  targetTopologyCompleteLatencyBefore=0.0;
            Double  targetTopologyCompleteLatencyAfter=0.0;
            Long  inputRateTopology=0L;


            long start =1 ;
            long end =1 ;
            float migrationduration =1;
            Double cost=1.0;
            int proper=1;



            if (elasticityComponentInfo != null) {
                topologyName = elasticityComponentInfo.getTopologyName();
                component = elasticityComponentInfo.getComponent();
                sourceSuperviorName = elasticityComponentInfo.getSourceSuperviorName();
                targetSuperviorName = elasticityComponentInfo.getTargetSuperviorName();
                sourceComponentCPUUsage = elasticityComponentInfo.getSourceCPUUsage();
                targetWorkerNodeCpuLoad = elasticityComponentInfo.getTargetWorkerNoneCpuLoad();
                sourceWorkerNodeCpuLoad = elasticityComponentInfo.getSourceWorkerNoneCpuLoad();
                inputRateTopology = elasticityComponentInfo.getInputRateTopolgy();

                start = elasticityComponentInfo.getStartTime();
                end = elasticityComponentInfo.getEndTime();
                migrationduration = (end - start) / 1000F;
                cost=0.0;



                if (migrationduration < 120) {
                    proper = 0;
                }else{
                    proper = 1;
                }


                System.out.println("......InsertTable.......migrationduration......" + migrationduration);
                System.out.println("......InsertTable.......inputRateTopology......" + inputRateTopology);
                System.out.println("......InsertTable.......proper......" + proper);

               // cost;
               // proper;
           }

            pstmt.setString(1, topologyName);
            pstmt.setString(2, component);
            pstmt.setString(3, sourceSuperviorName);
            pstmt.setString(4, targetSuperviorName);
            pstmt.setDouble(5, sourceComponentCPUUsage);
            pstmt.setDouble(6, targetWorkerNodeCpuLoad);
            pstmt.setDouble(7, sourceWorkerNodeCpuLoad);
            pstmt.setFloat(8, migrationduration);
            pstmt.setDouble(9, cost);
            pstmt.setInt(10, proper);
            pstmt.setLong(11, inputRateTopology);


            pstmt.executeUpdate();
            pstmt.close();
            System.out.println("A record has been saved into DB");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }



    public static ResultSet  selectFromTables( String sqlQuery ){

        Statement stmt = null;

        ResultSet recordSet =null;
        try{


            stmt = conn.createStatement();
            //ResultSet rs = stmt.executeQuery( "SELECT * FROM MachineLearningModel;" );
            recordSet = stmt.executeQuery( sqlQuery);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


        return recordSet;
    }



    public static void createTable() {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:SqliteJavaDB.db");
            System.out.println("Database Opened...\n");
            stmt = c.createStatement();
            String sql = "CREATE TABLE Product " +
                    "(p_id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    " p_name TEXT NOT NULL, " +
                    " price REAL NOT NULL, " +
                    " quantity INTEGER) " ;
            stmt.executeUpdate(sql);
            stmt.close();
            c.close();
        }
        catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        System.out.println("Table Product Created Successfully!!!");
    }

    public void update(int id, String name, double capacity) {
        String sql = "UPDATE warehouses SET name = ? , "
                + "capacity = ? "
                + "WHERE id = ?";

        try (
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setString(1, name);
            pstmt.setDouble(2, capacity);
            pstmt.setInt(3, id);
            // update
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }




    }
