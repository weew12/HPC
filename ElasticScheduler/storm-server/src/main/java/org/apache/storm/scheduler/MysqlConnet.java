package org.apache.storm.scheduler;


import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.apache.storm.scheduler.adaptive.DataManager;

import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;




public class MysqlConnet {

    public static DataManager instance = null;

    public PoolingDataSource dataSource;
    public Logger logger;
    public String nodeName;
    public int capacity; // the capacity of a node, expressed in percentage wrt the total speed
    public static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

//        public void mysqlconnect() {
//
//
//        }

//
    public void mysqlcnn() {


        System.out.println("dataManager ........");
        System.out.println("dataManager ........");
        System.out.println("dataManager ........");

        logger = Logger.getLogger(DataManager.class);
        logger.info("Starting DataManager (working directory: " + System.getProperty("user.dir") + ")");
        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        System.out.println("Starting DataManager (working directory......... " + System.getProperty("user.dir") + ")");



        System.out.println("dataManager ........");

        logger = Logger.getLogger(DataManager.class);
        logger.info("Starting DataManager (working directory: " + System.getProperty("user.dir") + ")");
        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        System.out.println("Starting DataManager (working directory......... " + System.getProperty("user.dir") + ")");

        boolean a=true;
       // if (a) return;

        try {
            // load configuration from file
            logger.debug("Loading configuration from file");
            Properties properties = new Properties();
            System.out.println("dataManager ..00......");
            //properties.load(new FileInputStream("/home/ali/db.ini"));
            properties.load(new FileInputStream("db.ini"));
            System.out.println("dataManager ..11......");
            logger.debug("Configuration loaded");

            logger.info("DataManager started");

                       // load JDBC driver
            logger.debug("Loading JDBC driver");
            String jdbc_driver = properties.getProperty("jdbc.driver").trim();
            System.out.println("jdbc_driver = " + jdbc_driver);
            Class.forName(jdbc_driver);
            logger.debug("Driver loaded");

                        // set up data source
            System.out.println("set up data source ..00......");
            logger.debug("Setting up pooling data source");
            String connection_uri = properties.getProperty("data.connection.uri");
            System.out.println("connection_uri = " + connection_uri);
            String validation_query = properties.getProperty("validation.query");
            System.out.println("validation_query = " + validation_query);
            ObjectPool connectionPool = new GenericObjectPool(null);
//            ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connection_uri, null);
//            PoolableConnectionFactory poolableConnectionFactory =
//            new PoolableConnectionFactory(connectionFactory, connectionPool, null, validation_query, false, true);
//            poolableConnectionFactory.hashCode();
//            dataSource = new PoolingDataSource(connectionPool);
//            logger.debug("Data source set up");




        } catch (Exception e) {
            logger.error("Error starting DataManager", e);
            System.out.println("dataManager ..22......");
        }




    }





}
