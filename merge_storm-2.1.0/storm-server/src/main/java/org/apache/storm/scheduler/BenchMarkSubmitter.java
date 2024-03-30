package org.apache.storm.scheduler;

//import org.apache.storm.daemon.nimbus;
import org.apache.storm.utils.ConfigUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

//import org.apache.storm.daemon.nimbus;
//import org.apache.thrift.TException;

//import org.apache.thrift.TException;
//import org.apache.storm.thrift.TException;

public  class BenchMarkSubmitter {

    public static Map<String, Object> config = ConfigUtils.readStormConfig();
    private static  Cluster cluster = null;

    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";

    private int interval = 4;
    private String component;
    private static String stream;
    //private String watch;
    private static String watch;

    private   Map<String, String>  supervisortotopology  =  new HashMap<>();

    public  Map<String, String> benchMarkSubmitter() {



        System.out.println("...........Thread.......BenchMarkSubmitter........Waiting");

        try {
            Thread.sleep(20000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }


        cluster = DefaultScheduler.getCluster();

        Map<String, SupervisorDetails> supervisordetails = cluster.getSupervisors();
        int supervisorsnumber = supervisordetails.size();


       // System.out.println(":::::Thread:::::Start Submitting BenchMark Topologies:::New::::  ");
       //for (int i=0 ; i<3 ;i++)
       // for (int i=0 ; i<supervisorsnumber ;i++)
        int i=0;
        for (  Map.Entry<String,  SupervisorDetails > entry : supervisordetails.entrySet()) {

           SupervisorDetails supervisordetail = entry.getValue();
           String id_supervisor= supervisordetail.getId();
           i++;
        try
        {
            Runtime rt = Runtime.getRuntime();
                       Process proc = rt.exec("storm jar  /home/work/apache-storm-2.1.0/bin/benchmark11.jar  com.mamad.wordcount.Main BenchMarkTask-"+i);


            supervisortotopology.put (id_supervisor,"BenchMarkTask-"+i);
            InputStream stdin = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdin);
            BufferedReader br = new BufferedReader(isr);

        } catch (Throwable t)
        {
            t.printStackTrace();
        }

           try {
               Thread.sleep(1000);
           } catch (InterruptedException ex) {
               Thread.currentThread().interrupt();
           }



       }

        //System.out.println(":::::Thread:::::Start Submitting BenchMark Topologies:::New::::  "+supervisortotopology);

        return supervisortotopology;

    }





    public void  benchMarkKiller() {



        System.out.println("...........Thread.......BenchMarkSubmitter........Killer");



        cluster = DefaultScheduler.getCluster();

        Map<String, SupervisorDetails> supervisordetails = cluster.getSupervisors();
        int supervisorsnumber = supervisordetails.size();


        int i=0;
        for (  Map.Entry<String,  SupervisorDetails > entry : supervisordetails.entrySet()) {

            SupervisorDetails supervisordetail = entry.getValue();
            String id_supervisor= supervisordetail.getId();
            i++;
            try
            {
                Runtime rt = Runtime.getRuntime();
                Process proc = rt.exec("storm kill  BenchMarkTask-"+i + " -w  1 ");


               // supervisortotopology.put (id_supervisor,"BenchMarkTask-"+i);
                InputStream stdin = proc.getInputStream();
                InputStreamReader isr = new InputStreamReader(stdin);
                BufferedReader br = new BufferedReader(isr);

            } catch (Throwable t)
            {
                t.printStackTrace();
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }



        }




    }







}
