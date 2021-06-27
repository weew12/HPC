package org.apache.storm.scheduler;

//import org.apache.storm.daemon.nimbus;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusClient;
//import org.apache.storm.daemon.nimbus;
import org.apache.storm.utils.NimbusClient;
//import org.apache.thrift.TException;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

//import org.apache.thrift.TException;
//import org.apache.storm.thrift.TException;

public  class TopologySubmitter {

    public static Map<String, Object> config = ConfigUtils.readStormConfig();
    private static  Cluster cluster = null;

    public  void topolgySubmitter() {


       //(new String[]{"your-program", "--password="+pwd, "some-more-options"});
        System.out.println(":::::Thread:::::topolgySubmitter::::Waiting:::::::  ");
        //mycluster =DefaultScheduler.mycluster ;

        try {
            Thread.sleep(20000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        try
        {
            Runtime rt = Runtime.getRuntime();
           // Process proc = rt.exec("dir");
          //  Process proc = rt.exec("storm list");
            Process proc = rt.exec("storm jar  /home/ali/work/apache-storm-2.1.0/bin/benchmark.jar  com.mamad.wordcount.Main AAA");

            InputStream stdin = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdin);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            System.out.println("<OUTPUT>");
            while ( (line = br.readLine()) != null)
                System.out.println(line);
            System.out.println("</OUTPUT>");
            int exitVal = proc.waitFor();
            System.out.println("Process exitValue: " + exitVal);
        } catch (Throwable t)
        {
            t.printStackTrace();
        }

        System.out.println(":::::Thread:::Before::waiting for deactivation::::Waiting:::::::  ");
        try {
            Thread.sleep(40000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        System.out.println(":::::Thread::After:::waiting for deactivation::::Waiting:::::::  ");

        DefaultScheduler defaultScheduler =new DefaultScheduler();
        cluster =defaultScheduler.getCluster();


        Collection<WorkerSlot> used_ports = cluster.getUsedSlots();
        for (WorkerSlot port : used_ports){
            System.out.println(":::::Thread::used_ports:::::::Waiting:::::::  "+port);
        }
        String topologyId="";
       for (TopologyDetails  topo : cluster.getTopologies()) {
            topologyId = topo.getId().toString();
           System.out.println(":::::Thread::used_ports:::::::topologyId:::::::  "+topologyId.toString());
       }
//
//        System.out.println(":::::Thread::used_ports:::::::Waiting:::::::  "+used_ports.size());
//        System.out.println(":::::Thread::used_ports:::::::Waiting:::::::  "+used_ports.toString());
//
//        System.out.println(":::::Thread::used_ports:::::::topologyId:::::::  "+topologyId.toString());

//        for (WorkerSlot port : used_ports)
//            System.out.println("::Thread::Default scheduler::::::USED_SLOTS::::::::::::: " + port.toString());
//             String id="";


        Map<String, Object> a = null;

       // UIHelpers uiHelpers =null;

        //UISer

         String id=topologyId;
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            //return (
            //System.out.println(":::::Thread::Inside try :::::::  ");
            try {
                putTopologyDeactivate(  nimbusClient.getClient(), id );
            } catch (Exception e) {
                 e.printStackTrace();
            }

           //;
        }

        System.out.println("Topology Dactivated ");
        System.out.println(":::::Thread::Before Activating  :::::::  ");

        try {
            Thread.sleep(60000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }




        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            //return (
           // System.out.println(":::::Thread::Inside try :::::::  ");
            try {
                putTopologyActivate(  nimbusClient.getClient(), id );
            } catch (Exception e) {
                e.printStackTrace();
            }

            //;
        }

        System.out.println(":::::Thread::Topology Activated :::::::  ");

        //System.out.println(":::::Thread::after try :::::::  ");
        //return;


       /* try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return (

                    try {
                        a = putTopologyDeactivate(nimbusClient.getClient(), id);
                    }  catch (Exception e) {
                    // e.printStackTrace();
                    }
                     // int a=0;
        });*/








       /* try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            //return UIHelpers.makeStandardResponse(
            // UIHelpers.putTopologyDeactivate(nimbusClient.getClient(), id),
            // callback




        }*/


    }



    public static Map<String, Object> putTopologyDeactivate(Nimbus.Iface client, String id) throws Exception {
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(id, getInfoOptions);
        client.deactivate(topologyInfo.get_name());
        return getTopologyOpResponse(id, "deactivate");
    }



    public static Map<String, Object> putTopologyActivate(Nimbus.Iface client, String id) throws Exception {
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(id, getInfoOptions);
        client.activate(topologyInfo.get_name());
        return getTopologyOpResponse(id, "activate");
    }




    public static Map<String, Object> getTopologyOpResponse(String id, String op) {
        Map<String, Object> result = new HashMap();
        result.put("topologyOperation", op);
        result.put("topologyId", id);
        result.put("status", "success");
        return result;
    }

}
