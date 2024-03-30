package org.apache.storm.scheduler;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.NimbusClient;

//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;

import java.util.*;
import java.util.Map.Entry;

public class BenchMarkGetInfo {


    private static  Cluster cluster = null;


    private static  Map<Double, Double> resultMetric  =  new HashMap<>();
    private static    Map<Double, Double>  resultMetricToComponenet  =  new HashMap<>();
    private   Map <String , Double > resultMetricTopology  =  new HashMap<>();
    public  static Map <String , Double > supervisorsBenchmarkExecute  =  new HashMap<>();
    private  static Map <String , Map<String, Double> > resultMetricTopologySupervisor  =  new HashMap<>();
    private static  Map <String , Map<Double, Double> > resultMetricSupervisor  =  new HashMap<>();
    private   Map<String, String>  supervisortotopology  =  new HashMap<>();
    public  static int numbeSupervisors =0;
    public  static boolean  fastCheck =false;



    public void wait (int miliseconds)
    {
        try {
            Thread.sleep(miliseconds);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

    }



    public  void benchMarkGetInfo() {

        BenchMarkSubmitter benchMarkSubmitter =new BenchMarkSubmitter();
        supervisortotopology =benchMarkSubmitter.benchMarkSubmitter();
        System.out.println(".........Thread.......BenchMarkGetInfo......Waiting................  "+ supervisortotopology);

        try {
            Thread.sleep(15000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }



        while(true) {

           // System.out.println("..................BenchMarkGetInfo..Loop........waiting 10 sec ...." );
            try {
                //Thread.sleep(120000);
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }

      DefaultScheduler defaultScheduler = new DefaultScheduler();
      cluster = defaultScheduler.getCluster();
      List<String> CandidtaeListForMigration = new ArrayList<String>();
      Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
      numbeSupervisors = supervisordetls.size();
      String id_supervisor = "";
         for (Map.Entry<String, SupervisorDetails> entry : supervisordetls.entrySet()) {
          id_supervisor = entry.getKey();
          //System.out.println("::::::::Thread:::::BenchMarkGetInfo:::::::::id_supervisor::::::" + id_supervisor);
      }
      String topologyId="";
      String topologyName="";


          resultMetricTopology.clear();
        for (TopologyDetails topology : cluster.getTopologies()) {
          try {
              Thread.sleep(1000);
          } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
          }
          topologyId = topology.getId().toString();
          topologyName = topology.getName().toString();
          Topologies topologies = cluster.getTopologies();

          if (topologyId.startsWith("BenchMarkTask")) {
              //Map <Double, Double>  resultfromExecuters = new HashMap<>();
              Map <Double, Double>  resultfromExecuters = new HashMap<>();
              resultfromExecuters = getExecuterMetric(topologies, cluster, topologyId, topologyName);
              //System.out.println("---------Loop-----resultfromExecuters--size-" + resultfromExecuters.size());
              Double capacity =0.0 ;
              Double executelatency=0.0;
              for (Entry<Double,Double> entry : resultfromExecuters.entrySet()) {
                   capacity =entry.getKey();
                   executelatency =entry.getValue();
               }
              Double execute=executelatency;
              Double cap=capacity;
              if (execute > 0) {
                  String supervisorID = topologyToSupervisors(topologyId);
                  // resultMetricTopology.put(topologyId, new HashMap(){{put(cpu,memory);}});
                  // resultMetricTopology.put(topologyId, execute);
                  resultMetricTopology.put(supervisorID, execute);
              }
          }
      }
           if (resultMetricTopology.size()  == supervisordetls.size() )
      {
           supervisorsBenchmarkExecute.putAll(resultMetricTopology);
          //System.out.println(":::::::::::::BenchMarkGetInfo:::::::::resultMetricTopology::::::" + resultMetricTopology);
         // BenchMarkChangeStatus.benchMarkChangeStatus(false);
          fastCheck=false;
      }
  }

 }


    public static   Map<Double, Double> getExecuterMetric(Topologies topologies, Cluster cluster, String topologyID, String topologyName) {


        Collection<WorkerSlot> workerslot =cluster.getUsedSlots();
        int num_worker = workerslot.size();
        MonitorScheduling monitor =new MonitorScheduling();
        if (num_worker > 0) {

            Integer interval = 4;
            String component = "";

            try {

                NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
                    @Override
                    public void run(Nimbus.Iface nimbus) throws Exception {
                        if ( num_worker == 0)  return;
                        HashSet<String> components = monitor.getComponents(nimbus, topologyName);
                       // System.out.println("Available  for " + topologyName + " :");
                      //  System.out.println("------------------");
                        MonitorScheduling.clearMetricToComponentBenchMarkTask();/// clear the map
                        for (String comp : components) {
                            if (comp.startsWith("splitter")   ) {
                                MonitorScheduling monitor =new MonitorScheduling();
                                String stream = "default";
                                String watch = "emitted";
                                monitor.setComponent(comp);
                                monitor.setStream(stream);
                                monitor.setWatch(watch);
                                monitor.setTopologyID(topologyID);
                                monitor.setTopology(topologyID);
                                resultMetric= monitor.metrics(nimbus);
                            }
                        }
                        resultMetricToComponenet = MonitorScheduling.getMetricToComponentBenchMarkTask();
                       // System.out.println("----------GetExecuterDetails-----resultMetricComponenet---"+ resultMetricToComponenet );
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return resultMetricToComponenet ;
    }


 public String topologyToSupervisors(String topology)
 {
     String  result= "";
     for (Entry<String ,String> entry : supervisortotopology.entrySet()) {
         String id_supervisor =entry.getKey();
         String topologysubmitted =entry.getValue();
         if (topology.contains(topologysubmitted)) {
             result = id_supervisor;
             break;
         }
     }
        return result;

 }




}
