package org.apache.storm.scheduler;


import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.generated.SupervisorSummary;

import java.util.*;


public  class Hmetrics {

    //private static final Logger LOG = LoggerFactory.getLogger(EvenScheduler.class);
   // public static Map<String, Object> config = ConfigUtils.readStormConfig();

    //static List<String> lst_topologies = new ArrayList<String>();
    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";

    private int interval = 4;
    private String component;
    private static String stream;
    //private String watch;
    private static String watch;

    public static Double CpuUsage=0.0;
    public static Double MemoryUsage=0.0;
    private static List<Supervisor> supervisors;



    public void setcpuusage(double cpuusage)
    {
        this.CpuUsage =cpuusage;
        //System.out.println("++++++++++++++++::setCPUUsage:::::: ++++++++++++++++"+cpuusage);
    }



    public  void display_metrics(Topologies topologies, Cluster cluster) {



        System.out.println("                                                                     ");
        System.out.println("++++++++++++++++::Hmetrics::::display_metrics:::::: ++++++++++++++++");

        boolean a =true;
        while (a ) {
            //System.out.println("                                                                     ");
            //System.out.println("++++++++++++++++::Hmetrics::::display_metrics:::::: ++++++++++++++++");
            try
            {
                Thread.sleep(5000);
            }
            catch(InterruptedException ex)
            {
                Thread.currentThread().interrupt();
            }

            SupervisorSummary supervisorSummary=null;


        }

        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        Set<WorkerSlot> slots = new HashSet<WorkerSlot>();

        boolean control = true;
        while (control) {


          Collection<WorkerSlot> used_ports = cluster.getUsedSlots();



          //System.out.println("*****************  slots *****************"+  Integer.toString(slots.size()) );

          for (WorkerSlot port : used_ports)
              System.out.println("::::Hmetrics::::::USED_SLOTS::::::::::::: " + port.toString());

          try
          {
              Thread.sleep(3000);
          }
          catch(InterruptedException ex)
          {
              Thread.currentThread().interrupt();
          }


      }


        for (TopologyDetails  topology : cluster.getTopologies()) {

                    String  topologyId =topology.getId().toString();

                  //String  newstr =topology.



                    Set<ExecutorDetails> allExecutors = topology.getExecutors();






            for (ExecutorDetails executor : allExecutors) {
                     }

        }



        for(int i=0;i<availableSlots.size();i++) {

            //System.out.println("AvailableSlots::" + availableSlots.get(i).toString());

        }


       /* if (num_worker  ==  1)
        {
            System.out.println("++++++++++++++++   ++++++++++++++++"+  Integer.toString( workerslot.size() ) );
            if (workerslot.size() == 1   &&   (!EvenScheduler.allow_reschedule)  )
            {
                System.out.println("++++++++++++++++  before waiting ++++++++++++++++");
                //sleep (10000);

              *//*  try
                {
                    Thread.sleep(30000);
                }
                catch(InterruptedException ex)
                {
                    Thread.currentThread().interrupt();
                }
*//*




            }

        }

*/





    }



    public static Set<WorkerSlot> slotsCanReassign(Cluster cluster, Set<WorkerSlot> slots) {
        Set<WorkerSlot> result = new HashSet<WorkerSlot>();
        for (WorkerSlot slot : slots) {
  //          System.out.println("++++++++++++++ slotsCanReassign:::slot +++++++++++" +  slot.getNodeId().toString());
            if (!cluster.isBlackListed(slot.getNodeId())) {
                SupervisorDetails supervisor = cluster.getSupervisorById(slot.getNodeId());
                if (supervisor != null) {
                    Set<Integer> ports = supervisor.getAllPorts();
                    if (ports != null && ports.contains(slot.getPort())) {
                        result.add(slot);
                    }
                }
            }
        }
        return result;
    }




    public static void call_monitor() throws Exception {


    }







}
