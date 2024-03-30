/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.supervisor.timer;

import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.healthcheck.HealthChecker;
import org.apache.storm.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class SupervisorHealthCheck implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorHealthCheck.class);
    private final Supervisor supervisor;

    public static Double CpuUsage=0.0;
    public static Double MemoryUsage=0.0;
    public static String  ComponentsUsageInfo= "";


    public SupervisorHealthCheck(Supervisor supervisor) {
        this.supervisor = supervisor;
    }

    @Override
    public void run() {
        Map<String, Object> conf = supervisor.getConf();
        LOG.info("Running supervisor healthchecks...");
        getUsageInfoFromHosts();
        //LOG.info(":::::::::::::hamid:::::::::::::::::::");
        int healthCode = HealthChecker.healthCheck(conf);
        if (healthCode != 0) {
            LOG.info("The supervisor healthchecks FAILED...");
            supervisor.shutdownAllWorkers(null, null);
            throw new RuntimeException("Supervisor failed health check. Exiting.");
        }
    }


    public static void getUsageInfoFromHosts() {


        ///
        //Double CpuUsage=0.0;
        //Double MemoryUsage=0.0;
       // System.out.println("++++++++++++++++::SupervisorHealthCheck:::::: +++++++getUsageInfoFromHosts+++++++++" );
      //  System.out.println("....... Test Supervisor..." );

        try
        {
            Runtime rt = Runtime.getRuntime();
            //  Process proc = rt.exec("dir");
            Process proc = rt.exec("top -b -n1");
            // Process proc = rt.exec("storm jar  /home/ali/work/apache-storm-2.1.0/bin/benchmark.jar  com.mamad.wordcount.Main AAA");

            InputStream stdin = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdin);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
           // System.out.println("<OUTPUT>");
            // LOG.info("<OUTPUT>");
            //System.out.println("<OUTPUT>");

            //br.re
            //String result="";
            int cnt=0;
            while ( (line = br.readLine()) != null)
            // System.out.println(line);
            {
//                result = result + line + "\t\n";
//                cnt++;
//                if (cnt > 5) break;

                if (line.startsWith("top") ){


                    String result = line.substring(line.indexOf("load average:") , line.length());
                    result=result.replace("load average:","");

                    String[] arrOfStr = result.split(",");
                    //String tmp=arrOfStr[1].toString();
                    String tmp=arrOfStr[0].toString();
                    CpuUsage= Double.parseDouble(tmp);
                   // System.out.println("::cpuusage::IP"+  " :::" + CpuUsage);




                }

                if (line.startsWith("KiB Swap") ) {
                    String result = line.substring(line.indexOf("used"), line.length());
                    result = result.replace("used.", "");
                    result = result.replace("avail Mem", "");
                    result = result.trim();

                    MemoryUsage= Double.parseDouble(result);
                    //System.out.println("::MemoryUsage::+IP" + IP +  ":::::" + MemoryUsage);
                }


            }

          //  System.out.println(":::::::::Functioin:::::::::CPU::::::::" + CpuUsage);
          //  System.out.println(":::::::::Functioin:::::::::MemoryUsage::::::::" + MemoryUsage);

            Hmetrics.CpuUsage= CpuUsage;
           Hmetrics.MemoryUsage= MemoryUsage;

            getExecuterCPUInformation();


//            HashSet<String> components = new HashSet<>();
//            Map<String, Double> resultMetric  =  new HashMap<>();
//            Map <String , Map<String, Double> > resultMetricToComponenet  =  new HashMap<>();
            //Cluster
          //  components = GetExecuterDetails.getComponentsName( DefaultScheduler.mycluster ,);
           // components =  GetExecuterDetails.getComponentsNameSupervisors( DefaultScheduler.mycluster );
          //  System.out.println(".....SupervisorHealthCheck....components.." + components);


           // System.out.println("" + Hmetrics.CpuUsage);
            //System.out.println(":::::::::Functioin:::::::::Hmetrics.MemoryUsage::::::::" + Hmetrics.MemoryUsage);

            // Hmetrics hmetrics= new Hmetrics();
             ///hmetrics.setcpuusage(CpuUsage);

            //DefaultScheduler.CpuUsage=CpuUsage;

            //result ="";


            //System.out.println("</OUTPUT>");
            int exitVal = proc.waitFor();
            // System.out.println("Process exitValue: " + exitVal);

        } catch (Throwable t)
        {
            t.printStackTrace();
        }




        ///

    }

    public static  void getExecuterCPUInformation() {

        long pidWorker =  getWorkerPID();
        //System.out.println("....... pidWorker..." + pidWorker);
        String topologyName ="" , topId ="";


        getWorkerInfo( pidWorker );
    }



    public static long getWorkerPID() {
        long pidWorker =0;

        ///

        try
        {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec("jps");
            InputStream stdin = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdin);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            int cnt=0;
            while ( (line = br.readLine()) != null)
            {
//                result = result + line + "\t\n";

                if (line.contains("Worker") ){
                   // System.out.println("  ...............  " + line.toString() );
                    String result ="";
                    result = line.replace("Worker", "");
                    result = result.trim();
                    long pid =0;
                    pid= Long.parseLong(result);
                  //  System.out.println("  .......PID........  " + pid );
                    pidWorker = pid;
                }
            }
            int exitVal = proc.waitFor();


        } catch (Throwable t)
        {
            t.printStackTrace();
        }

        return pidWorker;
    }




    public static long getWorkerInfo(long workerPID ) {
        long  pidWorker = workerPID;
        HashSet<String> components = new HashSet<>();
        Map<String, Double> resultMetric  =  new HashMap<>();
        Map <String , Map<String, Double> > resultMetricToComponenet  =  new HashMap<>();
       // components = GetExecuterDetails.getComponentsName( cluster , topologyName);

        //  GetExecuterDetails .getComponentsNameSupervisors( cluster );
        try
        {
            Runtime rt = Runtime.getRuntime();
            String command ="top -H -p " + pidWorker +  "  -b -n1 -w 512";
            Process proc = rt.exec(command );
            InputStream stdin = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdin);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            int cnt=0;

            ComponentsUsageInfo ="";
          //  System.out.println("  Supervisor 2  "  );
            while ( (line = br.readLine()) != null)

            {
                 //System.out.println("    " + line.toString() );
                if (line.contains("Thread") ){
                    String[] arrOfStr = line.split("-");
                    String selectedOperator ="";
                    if (arrOfStr.length == 3)
                    {
                        String operatorNameOs=arrOfStr[2].toString();
                  //      if (!operatorNameOs.contains("__ack")  && !operatorNameOs.contains("log") && !operatorNameOs.contains("sente") && !operatorNameOs.contains("__sys")) {
                        if (!operatorNameOs.contains("__ack")   && !operatorNameOs.contains("sente") && !operatorNameOs.contains("__sys")) {
                                  selectedOperator = operatorNameOs;
                                  line=line.replace("       ","@");
                                  line=line.replace("   ","@");
                                  line=line.replace("  ","@");
                                  line=line.replace(" ","@");
                                  String[] stringCPUMemory = line.split("@");
                                Double CPUUsage = Double.parseDouble(stringCPUMemory[stringCPUMemory.length-4].trim());
                                Double memoryUsage = Double.parseDouble(stringCPUMemory[stringCPUMemory.length-3].trim());
                                //System.out.println("....... ..." + selectedOperator +"..CPU.." + CPUUsage + "..Memory.." + memoryUsage);

                                ComponentsUsageInfo = ComponentsUsageInfo + selectedOperator+"@"+CPUUsage +"@"+memoryUsage + "/";
                          //  }
                        }
                    }

                }

            }
            ComponentsUsageInfo = "%%"+ComponentsUsageInfo;

        } catch (Throwable t)
        {
            t.printStackTrace();
        }
        //System.out.println("....... resultMetricToComponenet..." + resultMetricToComponenet);
       // System.out.println("....... ComponentsUsageInfo..." + ComponentsUsageInfo);
        return pidWorker;
    }






}
