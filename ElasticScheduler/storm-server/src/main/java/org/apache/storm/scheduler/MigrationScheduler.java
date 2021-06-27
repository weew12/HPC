package org.apache.storm.scheduler;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class MigrationScheduler {



    private static ISupervisor supervisor;
    private static final String SYSTEM_COMPONENT_PREFIX = "__";
    private static boolean ContinueProgram=true;
    private static boolean oneTimeAssignment=false;
    public static List<ExecutorDetails> executorToAssignOnTotal = new ArrayList<ExecutorDetails>();
    public static Map<WorkerSlot ,List<ExecutorDetails>> result = new HashMap<>();

    public static List<ExecutorDetails>    executorToAssignOnSourceWS = new ArrayList<ExecutorDetails>();
    public static List<ExecutorDetails>    executorToAssignOnTargetWS = new ArrayList<ExecutorDetails>();
    public static  WorkerSlot sourceWorkerSlot;
    public static  WorkerSlot targetWorkerSlot;




    public static void commitReturnMigration(Topologies topologies, Cluster cluster , String topolgyId,String component, List<ExecutorDetails> executorTotalOnSource ,WorkerSlot workerslotTarget ,WorkerSlot workerslotSource  ) {



        cluster.freeSlot(workerslotSource);
        cluster.freeSlot(workerslotTarget);
        //cluster.unassign(topolgyId);
        //cluster.unassign()

            cluster.assign(workerslotTarget, topolgyId, executorTotalOnSource);
        System.out.println("reverse migration is \"done\"");


    }



    public static Map<WorkerSlot ,List<ExecutorDetails>> commitExecuterMigration(Topologies topologies, Cluster cluster , String topolgyId,String component,String targetSupervisor , WorkerSlot workerslot  ) {

        WorkerSlot migrationWorkerSlot =null;
        long start = System.currentTimeMillis();
        result.clear() ;

        Map<String, SchedulerAssignment> assignments = cluster.getAssignments();
        TopologyDetails topology;
        SchedulerAssignment assignment;

         String topologyID= topolgyId;
         List<AugExecutorContext> moveableExecutors = getMoveableExecutorsContext(cluster,topologies.getById(topologyID),  cluster.getAssignmentById(topologyID),component);

         topology = topologies.getById(topologyID);

          assignment = assignments.get(topologyID);
          boolean newAssignmentDone =false;

            for(AugExecutorContext exCtx : moveableExecutors) {

                WorkerSlot localSlot = exCtx.getAugExecutor().getWorkerSlot();
                List<ExecutorDetails> source = new ArrayList<ExecutorDetails>();
                List<ExecutorDetails> target = new ArrayList<ExecutorDetails>();
                //System.out.println("before error 2 " );
                for (AugExecutorDetails aExec : exCtx.getNeighborExecutors()) {
                    if (exCtx.isTarget(aExec)) target.add(aExec.getExecutor());
                    else source.add(aExec.getExecutor());
                }

            }

             // System.out.println("localSlot  00" );

            for (AugExecutorContext executorContext : moveableExecutors) {
                Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
                String nodeId="";
                for (  Map.Entry<String,  SupervisorDetails > entry : supervisordetls.entrySet()) {
                    nodeId = entry.getKey();
                }
                WorkerSlot chosenWorkerslot= workerslot;
                targetWorkerSlot =  workerslot;
                if (chosenWorkerslot == null ) {
                    continue;
                }
                 newAssignmentDone = commitNewExecutorAssignment(
                        chosenWorkerslot,
                        executorContext.getAugExecutor(),
                        topology,
                        cluster);
            }

        System.out.println(" ..................................................................");

        executorToAssignOnTotal.removeAll(executorToAssignOnTargetWS);
        executorToAssignOnSourceWS.addAll(  executorToAssignOnTotal );
        int i=0;
        int numberTransferedExecuters= executorToAssignOnTargetWS.size()/2;   /// we transfer half of the executers of tasks

        for(ExecutorDetails executor : executorToAssignOnTargetWS)
        {
           if (i< numberTransferedExecuters) {
               executorToAssignOnSourceWS.add(executor);
               executorToAssignOnTargetWS.remove(executor);
           }
           i++;
        }

        try {

            cluster.unassign(topology.getId());
            cluster.freeSlot(sourceWorkerSlot);
            if (!executorToAssignOnSourceWS.isEmpty())
                cluster.assign(sourceWorkerSlot, topology.getId(), executorToAssignOnSourceWS);


            cluster.freeSlot(targetWorkerSlot);
            if (!executorToAssignOnTargetWS.isEmpty())
                cluster.assign(targetWorkerSlot, topology.getId(), executorToAssignOnTargetWS);

            for(ExecutorDetails executor : executorToAssignOnTargetWS)
                System.out.println("Migrated "+  (executor) );

            System.out.println(" ...................................." );

            for(ExecutorDetails executor : executorToAssignOnSourceWS)
               System.out.println("Remaining "+  (executor) );

          //  AugExecutorDetails ex = executorContext.getAugExecutor();
                System.out.println("Migration done (sys time: " + System.currentTimeMillis() + ")");
              //  System.out.println("Migrated " + ex.getExecutor() + " " + ex.getComponentId());
                System.out.println("Migrated " + component);
                System.out.println("From     " + sourceWorkerSlot);
                System.out.println("To       " + targetWorkerSlot);
                System.out.println("migration is \"done\"");

            result.put(sourceWorkerSlot,executorToAssignOnTotal);
        }
        catch (RuntimeException ex)
        {
            System.out.println("[EXECUTE] [RESULT] got exception " + ex.getClass().getSimpleName());
            ex.printStackTrace();
           // return false;
        }
        catch (Exception ex)
        {
            System.out.println("[EXECUTE] [RESULT] got exception " + ex.getClass().getSimpleName());
            ex.printStackTrace();
           // return false;
        }

        long end = System.currentTimeMillis();
        long elapsed = end - start;
        return result;

        }





    private static boolean commitNewExecutorAssignment(
            WorkerSlot targetWorkerSlot,
            AugExecutorDetails executorToMigrate,
            TopologyDetails topology,
            Cluster cluster)

    {
        boolean CONFIG_DEBUG=true;

        SchedulerAssignment assignment = cluster.getAssignmentById(topology.getId());
      //  System.out.println("   ....commitNewExecutorAssignment..assignment....."+  (assignment.toString() ) );
        ExecutorDetails executorToMove = executorToMigrate.getExecutor();
        Map<ExecutorDetails, WorkerSlot> ex2ws = assignment.getExecutorToSlot();
         sourceWorkerSlot = ex2ws.get(executorToMove);

        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        String targetslot= "";

        executorToAssignOnTotal.clear() ;
        for(ExecutorDetails executor : ex2ws.keySet())
        {


            executorToAssignOnTotal.add(executor) ;
           // System.out.println("   ...commitNewExecutorAssignment...executor.....Total....."+  (executor.toString() ) );
            // executor to move must be assigned to target worker slot
            if(executor.equals(executorToMove)) {
                executorToAssignOnTargetWS.add(executor);
                continue;
            }

            WorkerSlot workerSlot = ex2ws.get(executor);


            if(workerSlot.equals(sourceWorkerSlot))
            {

             }
            else if(workerSlot.equals(targetWorkerSlot))
            {
                // once target worker slot is freed, executor must be reassigned
                executorToAssignOnTargetWS.add(executor);
            }
            //executorToAssignOnTotal.add(executorToAssignOnTargetWS.ne) ;

        }


        try {



        }
        catch (RuntimeException ex)
        {
            System.out.println("[EXECUTE] [RESULT] got exception " + ex.getClass().getSimpleName());
            ex.printStackTrace();
            return false;
        }
        catch (Exception ex)
        {
            System.out.println("[EXECUTE] [RESULT] got exception " + ex.getClass().getSimpleName());
            ex.printStackTrace();
            return false;
        }






        return true;

    }



    //private List<AugExecutorContext> getMoveableExecutorsContext(TopologyDetails topology, GeneralTopologyContext context , SchedulerAssignment assignment)
    private static List<AugExecutorContext> getMoveableExecutorsContext(Cluster cluster,TopologyDetails topology,  SchedulerAssignment assignment,String component)
    {
        List<AugExecutorContext> moveableExecutors = new ArrayList<AugExecutorContext>();


        // preparing stuff
        if(assignment == null) return moveableExecutors;
        Map<ExecutorDetails, WorkerSlot> executorToWorkerSlot = assignment.getExecutorToSlot();

        if(executorToWorkerSlot == null || executorToWorkerSlot.isEmpty()) return moveableExecutors;
        Map<ExecutorDetails, WorkerSlot> localExecutorToWorkerSlot = new HashMap<ExecutorDetails, WorkerSlot>();

        //hmd
              // SupervisorDetails supervisor = cluster.getSupervisorById(slot.getNodeId());
             //  String supervisor_id = supervisor.getId() ;
           Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
           String localNodeID="";
           for (  Map.Entry<String,  SupervisorDetails > entry : supervisordetls.entrySet()) {

                localNodeID = entry.getKey();
           }

         //hmd




        // still preparing..
        for(ExecutorDetails exec : executorToWorkerSlot.keySet())
        {
            WorkerSlot wSlot = executorToWorkerSlot.get(exec);


            if(wSlot != null && localNodeID.equals(wSlot.getNodeId()))


           System.out.println(".........getMoveableExecutorsContext........  " + exec.toString() );

            localExecutorToWorkerSlot.put(exec, wSlot);


        }

        boolean aRelatedComponentIsMigrating;
        boolean currentComponentIsLeafNode;
        boolean currentComponentIsRootNode;

        List<String> sourceComponents;
        List<String> targetComponents;
        for(ExecutorDetails localExecutor : localExecutorToWorkerSlot.keySet()) {
            /*	localExecutor cannot be moved if its corresponding localComponent:
             *  -	is System Component (null id or starts with "__")
             *  - 	is migrating already
             *  -   has source or target component which is migrating
             *  -	is a leaf node (all target are System Component)
             *  -	is a root node (all source are System Component)
             */
            String componentID = topology.getExecutorToComponent().get(localExecutor);


            //topology.getExecutors().add() ;


           // System.out.println(" processing topology 77 :::componentID  " + componentID);

            // avoid null components and pinned components
            if(componentID == null || componentID.startsWith(SYSTEM_COMPONENT_PREFIX))
                continue;

            AugExecutorContext augExecContext = new AugExecutorContext(new AugExecutorDetails(localExecutor, topology, assignment));
            //augExecContext.addNeighbors(topology, context, assignment);
           //  System.out.println(" processing topology 88 :::augExecContext  " + augExecContext.toString());

            ///hmd
            if (componentID.equals(component) == true) moveableExecutors.add(augExecContext);
            /// hmd



        }

        return moveableExecutors;
    }



    public static void  rebalanceComponent(String topologyName ,String component  , int parallelizationDegree ) {



        System.out.println("............................... ");

        String relocateComponent ="";
        String topo =topologyName;
        int componentParallelDegree= parallelizationDegree;
        relocateComponent =component;


            try
            {
                Runtime rt = Runtime.getRuntime();
                String command =" storm rebalance " + topo  + " -w 1  -e "  + relocateComponent  + "=" + componentParallelDegree;
                System.out.println("..............rebalance........command......... "+ command);
                Process proc = rt.exec(command );
                InputStream stdin = proc.getInputStream();
                InputStreamReader isr = new InputStreamReader(stdin);
                BufferedReader br = new BufferedReader(isr);
            /*String line = null;
            System.out.println("<OUTPUT>");
            while ( (line = br.readLine()) != null)
                System.out.println(line);
            System.out.println("</OUTPUT>");
            int exitVal = proc.waitFor();
            System.out.println("Process exitValue: " + exitVal);*/
            } catch (Throwable t)
            {
                t.printStackTrace();
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }

        System.out.println("rebalance  is  \"done\"");

    }



    public static void  rebalanceComponentElasticity(String topologyName , String component  ) {




        System.out.println("............rebalanceComponentElasticity................... ");

        String relocateComponent ="";
        String topo =topologyName;
        int componentParallelDegree= 1;
        relocateComponent =component;


        try
        {
            Runtime rt = Runtime.getRuntime();
            String command =" storm rebalance " + topo  + " -w 1  -e "  + relocateComponent  + "=" + componentParallelDegree;
            System.out.println("..............rebalance........command......... "+ command);
            Process proc = rt.exec(command );
            InputStream stdin = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdin);
            BufferedReader br = new BufferedReader(isr);

        } catch (Throwable t)
        {
            t.printStackTrace();
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Reverse  rebalance  is  \"done\"");




    }




}




