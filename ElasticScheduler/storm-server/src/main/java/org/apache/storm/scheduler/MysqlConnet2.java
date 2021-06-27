package org.apache.storm.scheduler;


import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.apache.storm.scheduler.adaptive.DataManager;

import java.text.DateFormat;
import java.text.SimpleDateFormat;


public class MysqlConnet2 {

    public static DataManager instance = null;

    public PoolingDataSource dataSource;
    public Logger logger;
    public String nodeName;
    public int capacity; // the capacity of a node, expressed in percentage wrt the total speed
    public static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void mysqlcnn() {


        ObjectPool connectionPool = new GenericObjectPool(null);

//        try {
//           // Class.forName("com.mysql.jdbc.Driver");
//            System.out.println("driver loaded ...");
//
//
//        }
//        catch ( e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//            System.out.println("ERR...in driver"+e);
//
//        }



}




    }






