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

public  class BenchMarkChangeStatus {

    public static Map<String, Object> config = ConfigUtils.readStormConfig();
    private static  Cluster cluster = null;

    public  static void benchMarkChangeStatus(boolean activate) {


        DefaultScheduler defaultScheduler =new DefaultScheduler();
        cluster =defaultScheduler.getCluster();
        //Collection<WorkerSlot> used_ports = cluster.getUsedSlots();
//        for (WorkerSlot port : used_ports){
//            System.out.println(":::::Thread::used_ports:::::::Waiting:::::::  "+port);
//        }
        String topologyId="";
        for (TopologyDetails  topo : cluster.getTopologies()) {

            topologyId = topo.getId().toString();
           // System.out.println(":::::Thread::used_ports:::::::topologyId:::::::  " + topologyId.toString());
            Map<String, Object> a = null;
            String id = topologyId;
            if (!activate)
            {
                        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
                            //return (
                            //System.out.println(":::::Thread::Inside try :::::::  ");
                            try {
                                putTopologyDeactivate(nimbusClient.getClient(), id);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    System.out.println("Topology Dactivated ");
            }else {
                    try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
                        //return (
                        // System.out.println(":::::Thread::Inside try :::::::  ");
                        try {
                            putTopologyActivate(nimbusClient.getClient(), id);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        //;
                    }
                    System.out.println(":::::Thread::Topology Activated :::::::  ");
                }


        }




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
