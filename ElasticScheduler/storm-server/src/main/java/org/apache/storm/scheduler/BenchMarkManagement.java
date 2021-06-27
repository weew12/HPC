package org.apache.storm.scheduler;

import java.util.*;
import java.util.Map.Entry;

public class BenchMarkManagement {

    public static  boolean FirstMigrate = false;




    public static void startbenchMarkManagement(Topologies topologies, Cluster cluster , List<String> CandidtaeListForMigration) {

        System.out.println(":::::::::::::::::StartbenchMarkManagement::::::");
        /// this function is called via getWorkerNodeInfo to start benchmarking

        /// this code is temporary and is replaced by the calling function


        // List<String> candidateList = new ArrayList<String>();
        WorkerSlot sourceWorkerSlot=null, targetWorkerSlot=null;

        Map< String,WorkerSlot> CandidateFreeSlots = new HashMap< String,WorkerSlot>();





        CandidateFreeSlots =getCandidateFreeSlots(  cluster ,  CandidtaeListForMigration);

        for (  Entry<String,WorkerSlot> entry : CandidateFreeSlots.entrySet()) {

           // String id_supervisor = entry.getKey();
            targetWorkerSlot = entry.getValue();
            System.out.println(":::::::::::::::::Map::::::" +  targetWorkerSlot+"::::" );

        }


        String topologyId="";
        for (TopologyDetails  topology : cluster.getTopologies()) {

             topologyId = topology.getId().toString();
            // System.out.println(":::::::::::::::::Map::::::" + topologyId.toString());
        }


*/

       // List<WorkerSlot> usedSlots = cluster.getUsedSlots();
        Collection<WorkerSlot> UsedSlots =cluster.getUsedSlots();
        //WorkerSlot nodePort =cluster.getUsedSlots();

        int num_worker = UsedSlots.size();
        System.out.println("::::::::::num_worker::::::::::: " +num_worker + "::getUsedSlots::"  + UsedSlots);


*/
        if (! topologyId.equals("") && (num_worker>0) && FirstMigrate == false ) {
            //System.out.println(":::::::::::::::::111::::::" );

            System.out.println(":::::::::::::::::waiting 45sec for migration For the First Time::::::" );
            try {
               // Thread.sleep(30000);
                Thread.sleep(45000);
                // Thread.sleep(5000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }



                    TopologyDetails topology;
                    topology = topologies.getById(topologyId);
                    SchedulerAssignment assignment = cluster.getAssignmentById(topology.getId());

                    // System.out.println(":::::::::::::::::assignment::::::" + assignment.toString());

                    Map<ExecutorDetails, WorkerSlot> ex2ws = assignment.getExecutorToSlot();
                    //  System.out.println(" [MOVEABLE] - - ex2ws  " + ex2ws.toString());

                    List<ExecutorDetails> executorToAssignOnTargetWS = new ArrayList<ExecutorDetails>();


                    for (ExecutorDetails executor : ex2ws.keySet()) {

                        executorToAssignOnTargetWS.add(executor);
                        sourceWorkerSlot = ex2ws.get(executor);
                        //System.out.println("::::::::executorToAssignOnTargetWS::::sourceWorkerSlot:::::::::::" +sourceWorkerSlot +":::List executor::"   + executor.toString());
                    }


                      if (FirstMigrate == false) {



                          Collection<WorkerSlot> UsedSlot =cluster.getUsedSlots();
                          num_worker = UsedSlot.size();
                          System.out.println("::::::::BenchMk::num_worker::::::::::: " +num_worker + "::getUsedSlots::"  + UsedSlot);

                        //cluster.freeSlot(sourceWorkerSlot);
                        cluster.unassign(topology.getId());
                        //cluster.freeSlot(sourceWorkerSlot);
                        System.out.println("::::::::FirstTime:::::::::sourceWorkerSlot Freed::::::" + sourceWorkerSlot);
                        cluster.assign(targetWorkerSlot, topology.getId(), executorToAssignOnTargetWS);

                        System.out.println("::::::::FirstTime:::::Transfered::to::targetWorkerSlot::::::" + targetWorkerSlot);
                        FirstMigrate = true;




                       }


        }






    }

    public static  Map< String,WorkerSlot> getCandidateFreeSlots( Cluster cluster , List<String> CandidtaeListForMigration) {

        //Map< String,Integer> CandidateFreeSlots = new HashMap< String,Integer>();
        Map< String,WorkerSlot> CandidateFreeSlots = new HashMap< String,WorkerSlot>();
        for (String id_supervisor : CandidtaeListForMigration) {
            //System.out.println(":::::::::::::::::id_supervisor::::::" + id_supervisor);
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            for (WorkerSlot slot : availableSlots) {
                //slot.getPort() ;
                String slotsport=slot.toString();
                //slotsport =  slot.toString();
                int port =slot.getPort();
                //WorkerSlot nodeport =slot.g
                if (slotsport.contains(id_supervisor))
                {
                     //System.out.println(":::::::::::::::::inside map:::workerslot:::" +  slot+"::::" + port);
                    CandidateFreeSlots.put(id_supervisor,slot);
                }
            }

        }




        return CandidateFreeSlots;
    }






}
