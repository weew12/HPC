
package org.apache.storm.scheduler;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.*;
import java.util.*;

public class GetExecuterCPUMXBean {




    public static  void getExecuterCPUInformation(Topologies topologies, Cluster cluster) {

        long pidWorker =  getWorkerPID();
       // System.out.println("....... pidWorker..." + pidWorker);


        String topologyName ="" , topId ="";

        for (TopologyDetails  topology : cluster.getTopologies()) {
            topologyName = topology.getName().toString();
            topId = topology.getId().toString();
            // if (topId.equals(sourcetopologyID)) topologyName = topology.getName() ;
        }

        getWorkerInfo( pidWorker ,cluster, topologyName);
    }



    public static  void getCPUMXBean() {

//        ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
//        bean.getCurrentThreadCpuTime( ) ;




         int sampleTime = 10000;
         ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
         RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
         OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
         Map<Long, Long> threadInitialCPU = new HashMap<Long, Long>();
         Map<Long, Float> threadCPUUsage = new HashMap<Long, Float>();
         long initialUptime = runtimeMxBean.getUptime();





        long upTime = runtimeMxBean.getUptime();

        Map<Long, Long> threadCurrentCPU = new HashMap<Long, Long>();
        ThreadInfo[] threadInfos = threadMxBean.dumpAllThreads(false, false);
        //ThreadInfo[] threadInfos = threadMxBean.du
       // threadMxBean.getThreadInfo( pidWorker );

        for (ThreadInfo info : threadInfos) {
            threadCurrentCPU.put(info.getThreadId(), threadMxBean.getThreadCpuTime(info.getThreadId()));
            info.getThreadName() ;


        }






// CPU over all processes
//int nrCPUs = osMxBean.getAvailableProcessors();
// total CPU: CPU % can be more than 100% (devided over multiple cpus)
        long nrCPUs = 1;
// elapsedTime is in ms.
        long elapsedTime = (upTime - initialUptime);
        for (ThreadInfo info : threadInfos) {
            // elapsedCpu is in ns
            Long initialCPU = threadInitialCPU.get(info.getThreadId());


            if (initialCPU != null) {
                long elapsedCpu = threadCurrentCPU.get(info.getThreadId()) - initialCPU;
                float cpuUsage = elapsedCpu / (elapsedTime * 1000000F * nrCPUs);
                threadCPUUsage.put(info.getThreadId(), cpuUsage);
            }
        }

// threadCPUUsage contains cpu % per thread
        System.out.println(threadCPUUsage);
// You can use osMxBean.getThreadInfo(theadId) to get information on every thread reported in threadCPUUsage and analyze the most CPU intentive threads



    }



    public static long getWorkerInfo(long workerPID ,  Cluster cluster, String topologyName) {
    long  pidWorker = workerPID;
        HashSet<String> components = new HashSet<>();
          Map<String, Double> resultMetric  =  new HashMap<>();
          Map <String , Map<String, Double> > resultMetricToComponenet  =  new HashMap<>();
        components = GetExecuterDetails .getComponentsName( cluster , topologyName);

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
            while ( (line = br.readLine()) != null)

            {
               // System.out.println("    " + line.toString() );
                if (line.contains("Thread") ){
                    String[] arrOfStr = line.split("-");
                    String selectedOperator ="";
                    if (arrOfStr.length == 3)
                    {
                        String operatorNameOs=arrOfStr[2].toString();
                        for (String comp : components) {
                           // System.out.println("....... GetCPUMXBEAN..." + comp);
                            if (comp.contains( operatorNameOs))
                            {
                                selectedOperator = comp;
                               // System.out.println("....... selectedOperator..." + selectedOperator);
                                String[] stringCPUMemory = line.split("  ");
                                Double CPUUsage = Double.parseDouble(stringCPUMemory[6].trim());
                                Double memoryUsage = Double.parseDouble(stringCPUMemory[7].trim());
                              //  System.out.println("....... ..." + selectedOperator +"..CPU.." + CPUUsage + "..Memory.." + memoryUsage);
                                resultMetric.put(CPUUsage.toString(),memoryUsage);
                                resultMetricToComponenet.put(selectedOperator,resultMetric);
                                //System.out.println("....... resultMetricToComponenet..." + resultMetricToComponenet);
                            }
                        }
                    }

                }

            }

        } catch (Throwable t)
        {
            t.printStackTrace();
        }
        // System.out.println("....... resultMetricToComponenet..." + resultMetricToComponenet);
         return pidWorker;
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
                    System.out.println("  ...............  " + line.toString() );
                    String result ="";
                    result = line.replace("Worker", "");
                    result = result.trim();
                    long pid =0;
                    pid= Long.parseLong(result);
                    System.out.println("  .......PID........  " + pid );
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










}
