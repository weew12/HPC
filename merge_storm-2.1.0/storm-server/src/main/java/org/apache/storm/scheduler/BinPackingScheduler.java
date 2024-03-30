package org.apache.storm.scheduler;

//import org.apache.storm.command.monitor;

import org.apache.storm.generated.WorkerResources;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.shade.com.google.common.collect.Sets;

import java.io.*;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class BinPackingScheduler {

    public static List<WorkerNodeUsageDetails> workerNodeUsageDetailsList = new ArrayList<WorkerNodeUsageDetails>();
    public static List<ElasticityComponentInfo> elasticityComponentInfosList = new ArrayList<ElasticityComponentInfo>();

    public static int numbeSupervisors=0;
    public static Double cpuUsageFirstThereshold= 3.00;
    public static Double cpuUsageSecondThereshold=3.00;
    public static Double cpuUsageTheresholdReturn=0.0;
    public static Double componetcapacityThereshold=60.0;
    public static Double componetCpuUsageTheresholdReturn=9.0;
    public static String  targetNodeMigration="";
    public static String  sourceNodeMigration="";
    public static TopologyDetails  glbtopologyDetailes= null;
    public static int    parallelizationDegree =2;
    public static boolean  isMigrating=false;
    public static boolean  reverseMigrating=false;
    public static  boolean FirstRun = false;
    public static  boolean FirstRunScheduling = false;
    public static  boolean rebalaceIsDone = false;


    public static int adjSourceworker, adjtargetworker;
    public static double  cpuUsage,  cpuThreshold;


    public static Map <String , Double >  supervisorsBenchmarkExecute = new HashMap<>();

    public static  boolean ongoingMigrationProcess = false;
    public static  boolean ongoingReverseMigrationProcess = false;

    public static  boolean Firstreversr = false;
    public static  boolean isInitialStageDone = false;
    //public static  boolean isInitialQvalueFromFile = true;
    public static  boolean isInitialQvalueFromFile = false;

    public static SchedulerAssignment originalAssignment;
    public static Double sourceTopologyCompleteLatencyBefore=0.0;
    public static Double gamma=0.8;
    public static Double alpha = 0.9;

   // public static Double alpha = 0.7;

    public static int epsilon = 2;   //  it is 0.2

    public static  int epochNumber = 1;
    public static  int MigrationNumber = 1;

    public static  int maxEpochNumberConvergannce = 1;


    public static Double sourceTopologyCompleteLatencyAfter=0.0;
    public static boolean isMigrationNeeded = false;
    public static boolean isBenchMarkeTasksKilled = false;

    public static  Map<String, Double> sortedCompareWorkernodes = new HashMap<>();
    //private static  boolean FirstRun = false;

    public static  Map <String , Map<Double, Double> > supervisorCapacity = new HashMap<>();
    public static  Map <String ,  Double > supervisorsRewardFreeCapacity = new HashMap<>();
    public static  Map <String ,  Map<Boolean, Double> > supervisorsRewardStage = new HashMap<>();
    public static  Map <String ,   Double > supervisorsQValue = new HashMap<>();


    public static  Map <String ,ExecutorDetails > executersToSupervisors = new HashMap<>();
    public static  Map <String , Map<Double, Double> > componentsResources = new HashMap<>();
    public static  Map <ExecutorDetails , Map<Double, Double> > executersResources = new HashMap<>();
    public static String topologyID = "";
    public static String topologyName = "";
    public static String componentNameMigrated = "";
    public static Map<ExecutorDetails, WorkerSlot> execToSlotOriginalAssignment = new HashMap<>();
    public static Map<ExecutorDetails, WorkerSlot> execToSlotAfterRebalance = new HashMap<>();
    public static Set<ExecutorDetails> topologexecuters = null;
    public static  Map <String ,LinkedList < ExecutorDetails> > supervisorsExecutersLinkedList = new HashMap<>();

    public static List < ExecutorDetails > newAddedExecsAfterRebalance = new ArrayList<ExecutorDetails>();
    public static List < ExecutorDetails > originalExecsBeforeRebalance = new ArrayList<ExecutorDetails>();
    public static List < ExecutorDetails > originalReplicatedExecuter = new ArrayList<ExecutorDetails>();

    public static Long  TheresholdInputRateMigration =15000L;
   // public static Long  TheresholdInputRateMigration =50000L;
    public static Integer  turnToMoveToOtherWorkerNode =0;


    public static  int testcounter = 0;


    public static  int testcounter2 = 0;
    public static  int testcounterMigration = 0;

    public static  Long previousTupleEmitted = 0L;
    public static  Long  inputRateTopology = 0L;
    public static  Long  inputRateTopologyMigration = 0L;
    public static  Double  TheresholdprobabilityToMigrate = 80.0;

    public static  Long  landa = 2000L;


    public static Map<String, List<ExecutorDetails>>   compToExecuters;
    public static List <ExecutorDetails> finalTargetComponentMigration =new ArrayList<ExecutorDetails >();
    public static List <String> tabooList =new ArrayList<String >();

    public static List<ActionEvaluation> lstActionEvaluation = new ArrayList<ActionEvaluation>();






    public static void  getWorkernodesInfo(Topologies topologies, Cluster cluster, Map<String, Map<String, String>> workloadUsageInfo) {

/// Monitor
        //System.out.println(":::::[1-5]:::::::getWorkernodesInfo:::::::::::::::::::::" + workloadUsageInfo.toString());
        String cpuusageStr  = "";
        String memoryStr  =   "";
        String memoryStrComponentUsage  =   "";
        String componentCPUMemory  =   "";
        String SupervisorId = "";
        workerNodeUsageDetailsList.clear() ;
        List <ComponentCPUMemoryInfo>  listcomponentCPUMemoryInfo = new  ArrayList<ComponentCPUMemoryInfo>();
        listcomponentCPUMemoryInfo.clear() ;
        for (Entry<String, Map<String, String>> entryusage : workloadUsageInfo.entrySet()) {
            SupervisorId = entryusage.getKey();
            Map<String, String> workloadcpumemory = entryusage.getValue();
            for (Entry<String, String> entryusage2 : workloadcpumemory.entrySet()) {
                cpuusageStr = entryusage2.getKey();
                memoryStrComponentUsage = entryusage2.getValue();
                // System.out.println("  .......memoryStrComponentUsage........  " + memoryStrComponentUsage  );

                /// seperating information of cpu usage and memory of each component
                String[] stringCPUMemory = memoryStrComponentUsage.split("%%");
                if ( stringCPUMemory.length ==1 ) {             /// if the information of components are attached
                    memoryStr = memoryStrComponentUsage;
                    memoryStr=memoryStr.replace("%%","");

                } else
                {
                    memoryStr = stringCPUMemory[0];
                    memoryStr=memoryStr.replace("%%","");
                    componentCPUMemory = stringCPUMemory[1];
                    String[] componentsArray = componentCPUMemory.split("/");

                    for (String comp : componentsArray ) {
                        ComponentCPUMemoryInfo componentCPUMemoryInfo =new ComponentCPUMemoryInfo();
                        String[] componentsInfoDetail = comp.split("@");
                        String operator = componentsInfoDetail[0];
                        String operatorCPUUsage = componentsInfoDetail[1];
                        String operatorMemoryUsage = componentsInfoDetail[2];
                        componentCPUMemoryInfo.supervisorId = SupervisorId;
                        componentCPUMemoryInfo.name = operator;
                        componentCPUMemoryInfo.CPUUsage = Double.parseDouble(operatorCPUUsage.toString());
                        cpuUsage = Double.parseDouble(operatorCPUUsage.toString());

                        componentCPUMemoryInfo.memoryUsage = Double.parseDouble(operatorMemoryUsage.toString());
                        listcomponentCPUMemoryInfo.add(componentCPUMemoryInfo);
                       //  System.out.println("  .......componentsInfoDetail........  " + operator +".." + operatorCPUUsage +".." + operatorMemoryUsage +".." );
                    }
                }

            }
            //System.out.println(":::::[1-3]:::::::getWorkernodesInfo:::::::::::::::::::::" + workloadUsageInfo.toString());
            WorkerNodeUsageDetails workerNodeUsageDetails =new WorkerNodeUsageDetails();
            workerNodeUsageDetails.setSupervisorID(SupervisorId);
            workerNodeUsageDetails.setCpuUsage(Double.parseDouble(cpuusageStr));
            workerNodeUsageDetails.setMemoryUsage(Double.parseDouble(memoryStr));
            workerNodeUsageDetailsList.add(workerNodeUsageDetails);
            // System.out.println("::::::::::::getWorkernodesInfo:::::::" + SupervisorId+"::::"+cpuusageStr+"::::"+memoryStr+"::::");

        }   /// all information about nodes and operators are gathered in the two list


        ArrayList<WorkerNodeUsageDetails> arraylist = new ArrayList<WorkerNodeUsageDetails>();
        Collections.sort(workerNodeUsageDetailsList,WorkerNodeUsageDetails.cpuusageComparator);
        Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
        numbeSupervisors = supervisordetls.size();

        schedule(   topologies,  cluster);

        checkWorkernodesThershold(workerNodeUsageDetailsList,listcomponentCPUMemoryInfo ,topologies, cluster );
        performMigration(cluster,topologies ,listcomponentCPUMemoryInfo ,false);    /// only test   this function is called in the getworkernodesInfo
        /// the arguments allowmigration is for control while testing the program
        elasticityReturnComponent( listcomponentCPUMemoryInfo,cluster,  topologies);

    }


    public static  void  schedule(  Topologies topologies, Cluster cluster) {

        // logger("..Online Scheduling Assignment "  + ".."+ exec +"..."+wslot);

         boolean a= true; if(a) return;
        String topologyId="";
        Topologies topologyCount= cluster.getTopologies();

        System.out.println("..Bin Packing.schedule.......... ..........." );

       //boolean a= true; if(a) return;

      //  System.out.println("..Bin Packing.schedule......topologyCount....  " + topologyCount + "..........." );
       if (!topologyCount.toString().contains("Mytopology")) return;
     //   System.out.println("..Bin Packing.schedule......after adding....  " + topologyCount + "..........." );




        if (!FirstRunScheduling ) {

            System.out.println("..Bin Packing.schedule.......... ..22........." );


            Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
            for (Entry<String, SupervisorDetails> entry : supervisordetls.entrySet()) {
                String id_supervisor = entry.getKey();
                SupervisorDetails supdetails = entry.getValue();
                String supervisorid = supdetails.getId();
                double totalcpu = supdetails.getTotalCpu();
                double totalmem = supdetails.getTotalMemory();
                Map<Double, Double> capacity = new HashMap<>();
                capacity.put(totalcpu, totalmem);
                supervisorCapacity.put(id_supervisor, capacity);


                LinkedList<ExecutorDetails> linkedlistExecuters = new LinkedList<>();    /// for detecting executers in each bin
                supervisorsExecutersLinkedList.put(id_supervisor, linkedlistExecuters);


               // System.out.println("..Bin Packing.Supervisor......totalcpu....  " + supervisorid + "..........." + totalcpu + "..." + totalmem);
            }
        }


        List<ExecutorDetails> orderedExecutors=null;
        for (TopologyDetails  topology : cluster.getTopologies()) {





            topologyId = topology.getId().toString();
            topologyID =  topology.getId().toString();

            System.out.println("..Bin Packing.schedule2......topologyID....  " + topologyID + "..........." );
            if (!topologyID.toString().contains("Mytopology")) continue;
          //  System.out.println("..Bin Packing.schedule3......topologyID....  " + topologyID + "..........." );
           // System.out.println("..Bin Packing.schedule..........  " + topologyID + "..........." );

           // " MytopologyTest"

            double cpuNeeded = topology.getTotalRequestedCpu();
            Map<String,Component> topologyComp =  topology.getComponents();
            Set<ExecutorDetails> topologexecuters =  topology.getExecutors();




            System.out.println("..Bin Packing.schedule3......topologyID." );




            //System.out.println("..Bin Packing.schedule..3..  "+ topologyCount);
            String topoId = topologyId;
            SchedulerAssignment assignment = cluster.getAssignmentById(topoId);
            RAS_Nodes nodes;
            nodes = new RAS_Nodes(cluster);
            System.out.println("..Bin Packing.schedule4......topologyID." );

            Map<String, String> superIdToRack = new HashMap<>();
            Map<String, String> superIdToHostname = new HashMap<>();
            Map<String, List<RAS_Node>> hostnameToNodes = new HashMap<>();


            for (RAS_Node node: nodes.getNodes()) {
                System.out.println("..Bin Packing.schedule5......topologyID." );
                String superId = node.getId();
                //  System.out.println("...RAS......superId....  "  + superId  );
                String hostName = node.getHostname();
                //  System.out.println("...RAS......hostName....  "  + hostName  );
                //String rackId = hostToRack.getOrDefault(hostName, DNSToSwitchMapping.DEFAULT_RACK);
                superIdToHostname.put(superId, hostName);
                // superIdToRack.put(superId, rackId);
                hostnameToNodes.computeIfAbsent(hostName, (hn) -> new ArrayList<>()).add(node);
                //  rackIdToNodes.computeIfAbsent(rackId, (hn) -> new ArrayList<>()).add(node);
            }


            System.out.println("..Bin Packing.schedule6......topologyID." );

                 if (topologyID.toString().contains("Mytopology"))
                 {
                    // System.out.println("..Bin Packing.Supervisor......topologyID.1...  " + topologyID);

                     System.out.println("..Bin Packing.schedule7......topologyID." );
                     getComponentPageInfo(  topologyID  ,cluster );

                 }




           // boolean a= true; if(a) return;

            System.out.println(".................schedule check Slots...8.....  " );
            Collection <WorkerSlot>  usedSlots = cluster.getUsedSlots() ;
            System.out.println(".................schedule check Slots........  "+ cluster.getUsedSlots());
            if (usedSlots.size() == 0) return ;
            System.out.println(".................schedule check Slots....size.9...  " + usedSlots.size());

            if (!FirstRunScheduling ) {




                List<WorkerSlot>   avilabslots = cluster.getAvailableSlots();

                System.out.println(".................Inside real scheduling ........  " );
                System.out.println(".................avilabslots ........  "+ avilabslots );
                for (WorkerSlot slot : avilabslots) {
                    cluster.freeSlot(slot);
                     System.out.println(".................avilabslots ........  "+ avilabslots );

                }

                isBenchMarkeTasksKilled =true;
                BenchMarkSubmitter benchMarkSubmitter =new BenchMarkSubmitter();
                benchMarkSubmitter.benchMarkKiller();
                cluster.unassign(topologyId);




                Collection<ExecutorDetails> unassignedExecutors = new HashSet  <>  (cluster.getUnassignedExecutors(glbtopologyDetailes));
                if (unassignedExecutors.size() ==0) {
                    System.out.println("..Bin Packing.Supervisor......unassignedExecutors null....  ");
                    return;
                }
                orderedExecutors = orderExecutors(glbtopologyDetailes, unassignedExecutors);
                System.out.println("..Bin Packing.Supervisor......orderedExecutors....  " + orderedExecutors);
                //System.out.println(".....Executors....executersResources..  " + executersResources);





                Collection <TopologyDetails> topologydetails= cluster.getTopologies().getTopologies();
                for (TopologyDetails td : topologydetails) {
                    compToExecuters = cluster.getNeedsSchedulingComponentToExecutors( td );
                    System.out.println("........Schedule....compToExecuters..."+ compToExecuters );
                }
                System.out.println("........Schedule....assign Executers..."+ compToExecuters );
                assignExecuters(topologies, cluster, orderedExecutors);

                FirstRunScheduling=true;
            }



        }  /// main for loop topologies

    }

    public static  void  initialQlearning(   ) {

        if ( !isInitialStageDone )
        {
            initialStage();



            if (isInitialQvalueFromFile)   /// when we want to reload the previous information of Qlearning value
            {
                supervisorQvalueInitilize("supervisorQvalue");
                isInitialQvalueFromFile = false;
            }else
            {
                initialQvalueMatrix();
            }



          //  System.out.println("....*************************************************************......");
            System.out.println("....supervisorsRewardStage......." + supervisorsRewardStage);
            System.out.println("....supervisorsQValue......" + supervisorsQValue.toString());
            isInitialStageDone=true;
        }

    }


    public static  void  qLearningProcess(   ) {


        updatesupervisorsRewardStage ( );
        saveAndEvaluateevaluateAction();
        updatesupervisorsQvalue ( );
    }


    public static void saveAndEvaluateevaluateAction(    )
    {

        List<ExecutorDetails>   executerDetails =null;
        String  sourceSupervisorID =null;
        String  targetSupervisorID =null;
        Double costValueBeforMigration =0.0;
        Double costValueAfterMigration =0.0;
        int epochnumber;
        //Long inputRate = 0L;



        ActionEvaluation actionEvaluation = new ActionEvaluation();
        actionEvaluation.executerDetails = finalTargetComponentMigration;
        actionEvaluation.sourceSupervisorID =sourceNodeMigration;
        actionEvaluation.targetSupervisorID = targetNodeMigration;
        actionEvaluation.costValueBeforMigration=  sourceTopologyCompleteLatencyBefore;
        actionEvaluation.costValueAfterMigration =sourceTopologyCompleteLatencyAfter;
        actionEvaluation.epochnumber = epochNumber;
        actionEvaluation.inputRate = inputRateTopology;
        actionEvaluation.execToSlotAfterRebalanceEvaluation.putAll(execToSlotAfterRebalance) ;



        System.out.println("..saveAndEvaluateevaluateAction.......executerDetails.." + finalTargetComponentMigration);
        System.out.println("..saveAndEvaluateevaluateAction.......sourceSupervisorID.." + sourceNodeMigration);
        System.out.println("..saveAndEvaluateevaluateAction.......targetSupervisorID.." + targetNodeMigration);
        System.out.println("..saveAndEvaluateevaluateAction.......costValueBeforMigration.." + sourceTopologyCompleteLatencyBefore);
        System.out.println("..saveAndEvaluateevaluateAction.......costValueAfterMigration.." + sourceTopologyCompleteLatencyAfter);
        System.out.println("..saveAndEvaluateevaluateAction.......epochnumber.." + epochNumber);
        System.out.println("..saveAndEvaluateevaluateAction.......inputRate.." + inputRateTopology);
        System.out.println("..saveAndEvaluateevaluateAction.......inputRate.." + actionEvaluation.execToSlotAfterRebalanceEvaluation);
        //System.out.println("..saveAndEvaluateevaluateAction.......finalTargetComponentMigration.." + finalTargetComponentMigration);

        lstActionEvaluation.add(actionEvaluation );




    }



    public static  boolean   isAllworkerNodesVisited() {

        Boolean allworkernodeVisited =true;

        for ( Entry <String , Map<Boolean, Double> > entry : supervisorsRewardStage.entrySet()) {

            String id_supervisor = "";

            Map<Boolean, Double> resources = null;
            id_supervisor = entry.getKey();
            resources = entry.getValue();
            int cnt=0;



            for (Entry <Boolean, Double> entry1 : resources.entrySet())
            {
                cnt++;
                Boolean isWorkerNodeVisited =entry1.getKey();
                // boolean isnodeoverutilized = isWorkerNodeOverUtilized( id_supervisor );
                if (  !isWorkerNodeVisited )
                {
                    allworkernodeVisited = false;
                    break;
                }
            }
        }

        System.out.println("...........isAllworkerNodesVisited........visitedWorkerNodeFound.........." + allworkernodeVisited );


        return allworkernodeVisited;

    }


    public static  void   updateEpochNumber() {
        epochNumber ++;
        System.out.println("...........updateEpochNumber........visitedWorkerNodeFound.........." + epochNumber );

            for ( Entry <String , Map<Boolean, Double> > entry : supervisorsRewardStage.entrySet()) {

                String id_supervisor = "";
                Double rewardSupervisor = 0.0;
                // Double memTotalSupervisor = 0.0;
                Map<Boolean, Double> resources = null;
                id_supervisor = entry.getKey();
                resources = entry.getValue();
                int cnt=0;
                for (Entry <Boolean, Double> entry1 : resources.entrySet())
                {
                    cnt++;
                    Boolean isWorkerNodeVisited =entry1.getKey();
                    //cpuFressCapacity = entry1.getValue();
                    Map<Boolean, Double> visitedCapacity = new HashMap<>();
                    visitedCapacity.put(false, rewardSupervisor );
                    supervisorsRewardStage.replace ( id_supervisor , visitedCapacity );
                }
            }
    }


    public static   List < String >   selectUnvisitedWorkerNodes(   ) {

        List < String > selectedWorkerList =new ArrayList< String >();

        System.out.println("..selectUnvisitedWorkerNodes.......numberWorkerNodes.." );


       boolean isallworkernodesVisited = isAllworkerNodesVisited();

       if ( isallworkernodesVisited == true ) {
           updateEpochNumber();
       }




        for ( Entry <String , Map<Boolean, Double> > entry : supervisorsRewardStage.entrySet()) {

            String id_supervisor = "";
            Map<Boolean, Double> resources = null;
            id_supervisor = entry.getKey();
            resources = entry.getValue();
            int cnt=0;


            for (Entry <Boolean, Double> entry1 : resources.entrySet())
            {
                cnt++;
                Boolean isWorkerNodeVisited =entry1.getKey();
                // boolean isnodeoverutilized = isWorkerNodeOverUtilized( id_supervisor );
                if (  !isWorkerNodeVisited )
                {
                    selectedWorkerList.add( id_supervisor );
                }
            }
        }

        System.out.println("..selectUnvisitedWorkerNodes.......selectedWorkerList.." + selectedWorkerList);
        return selectedWorkerList;

    }


    public static  String   selectAction(  boolean movefromotherworkernode ) {





        String RandomSupervisorid ="";
        List < String > selectedWorkerList =new ArrayList< String >();


        selectedWorkerList =  selectUnvisitedWorkerNodes();




        int numberWorkerNodes = selectedWorkerList.size();
        System.out.println("..selectAction.......numberWorkerNodes.." + numberWorkerNodes);
        // if (numberWorkerNodes ==0 ) return RandomSupervisorid;


        if (  numberWorkerNodes >0  ) {    /// when there exist some worker nodes that have not met yet until now

            Random random = new Random();
            int rand = 0;
            rand = random.nextInt(numberWorkerNodes);
            System.out.println("..selectAction.rand.." + rand);


            if ( movefromotherworkernode)
            {

                /// when we move from one worker node without performing reverse migration to other node when inpur rate increase
                /// for preventing to move to the current worker node
                while(true)
                {
                    selectedWorkerList =  selectUnvisitedWorkerNodes();
                    System.out.println("..movefromotherworkernode  .....true......selectedWorkerList" + selectedWorkerList);
                    if (!selectedWorkerList.equals(targetNodeMigration)) break;
                }

            }else    /// for normal migration, reverse migration
            {
                  RandomSupervisorid = selectedWorkerList.get(rand);
            }


            System.out.println("..selectAction.rand.." + rand);
            System.out.println("..selectAction.supervisorid.." + RandomSupervisorid);


            for (Entry<String, Map<Boolean, Double>> entry : supervisorsRewardStage.entrySet()) {

                String id_supervisor = "";
                Double rewardSupervisor = 0.0;
                // Double memTotalSupervisor = 0.0;
                Map<Boolean, Double> resources = null;
                id_supervisor = entry.getKey();
                resources = entry.getValue();
                int cnt = 0;


                if (id_supervisor.equals(RandomSupervisorid)) {

                    for (Entry<Boolean, Double> entry1 : resources.entrySet()) {
                        cnt++;
                        Boolean isWorkerNodeVisited = entry1.getKey();
                        rewardSupervisor = entry1.getValue();
                        // boolean isnodeoverutilized = isWorkerNodeOverUtilized( id_supervisor );
                        if (!isWorkerNodeVisited) {
                            Map<Boolean, Double> visitedCapacity = new HashMap<>();
                            visitedCapacity.put(true, rewardSupervisor);
                            supervisorsRewardStage.replace(id_supervisor, visitedCapacity);
                            //selectedWorkerList.add(id_supervisor);
                            // supervisorid =  id_supervisor ;

                        }
                    }

                }
            }
            System.out.println("..selectAction.......supervisorsRewardStage.." + supervisorsRewardStage.toString());

        }

        return  RandomSupervisorid;

    }



    public static  String   changeMigrationToPermananetScheduling ( )
    {
        String id_supervisorSelected ="";
        int cnt=0;
        Double firstResponseTime =0.0;
        int foundBestResult =0;



        for (ActionEvaluation actionEvaluation : lstActionEvaluation) {

            List<ExecutorDetails>   executerDetails =actionEvaluation.executerDetails;
            String  sourceSupervisorID = actionEvaluation.sourceSupervisorID ;
            String  targetSupervisorID =actionEvaluation.targetSupervisorID;
            Double costValueBeforMigration =actionEvaluation.costValueBeforMigration;
            Double costValueAfterMigration =actionEvaluation.costValueAfterMigration;
            //actionEvaluation.

            int epochnumber = actionEvaluation.epochnumber;
            Long inputRate = actionEvaluation.inputRate;

            if (cnt == 0)
            {
                firstResponseTime =  costValueBeforMigration;
            }else
            {
              if(   costValueAfterMigration < firstResponseTime)
              {
                     firstResponseTime =  costValueAfterMigration ;
                     foundBestResult =cnt;
              }
            }



            System.out.println("..getBestMigrationValue.......cnt.." + cnt);
            System.out.println("..getBestMigrationValue.......executerDetails.." + executerDetails);
            System.out.println("..getBestMigrationValue.......sourceSupervisorID.." + sourceSupervisorID);
            System.out.println("..getBestMigrationValue.......targetSupervisorID.." + targetSupervisorID);
            System.out.println("..getBestMigrationValue.......costValueBeforMigration.." + costValueBeforMigration);
            System.out.println("..getBestMigrationValue.......costValueAfterMigration.." + costValueAfterMigration);
            System.out.println("..getBestMigrationValue.......epochnumber.." + epochnumber);
            System.out.println("..getBestMigrationValue.......inputRate.." + inputRate);
            System.out.println("........................................................................." );

            cnt++;



        }


        if ( foundBestResult >0 ) {   /// when we find a evalution track that has response time better than relocation
            ActionEvaluation permanetScheduling = lstActionEvaluation.get(cnt);
            id_supervisorSelected = permanetScheduling.targetSupervisorID;
        }

        System.out.println("..getBestMigrationValue.......inputRate.." + firstResponseTime);

        return id_supervisorSelected;

    }



    public static  String   getBestsupervisorsQvalue ( )
    {
             /// after convergance we get the best result for the supervisor
       // Double qValueStateAction=0.0;
        Double maxQValueNextActionAllActions=0.0;
        String id_supervisorSelected ="";

        System.out.println("....getBestsupervisorsQvalue... supervisorsQValue...." + supervisorsQValue);

        /// we calculate all possible qvalues for all possible actions except the current state
        for ( Entry <String ,  Double > entry : supervisorsQValue.entrySet()) {

            Double qValue = 0.0;
            // Double memTotalSupervisor = 0.0;
            String id_supervisor = entry.getKey();
            qValue = entry.getValue();
            /// qvalue update
            if (  qValue > maxQValueNextActionAllActions ) {

                maxQValueNextActionAllActions = qValue;
                id_supervisorSelected = id_supervisor;

            }
        }

        System.out.println("....getBestsupervisorsQvalue... id_supervisorSelected...." + id_supervisorSelected);
        return id_supervisorSelected;


    }


    public static  void  updatesupervisorsQvalue ( )
    {

      //  supervisorsQValue


        /// calculating other possibilities

        Double qValueStateAction=0.0;
        Double maxQValueNextActionAllActions=0.0;


        System.out.println("....updatesupervisorsQvalue... supervisorsQValue...." + supervisorsQValue);

        /// we calculate all possible qvalues for all possible actions except the current state
        for ( Entry <String ,  Double > entry : supervisorsQValue.entrySet()) {

            String id_supervisor = "";
            Double qValue = 0.0;
            // Double memTotalSupervisor = 0.0;
            id_supervisor = entry.getKey();
            qValue = entry.getValue();
            /// qvalue update
            if ( ! id_supervisor.equals(targetNodeMigration) ) {


                /// we select maximum all possibilities
                if (  qValue > maxQValueNextActionAllActions)    maxQValueNextActionAllActions = qValue;
                System.out.println("....updatesupervisorsQvalue... id_supervisor..."+"....qValue.." + qValue );
            }else
            {
                qValueStateAction = qValue;    /// we retrieve the value of the current satae
                System.out.println("....updatesupervisorsQvalue...currentstateQvalue " + qValueStateAction );

            }



        }



        System.out.println("....updatesupervisorsQvalue...Max qValueNextActionAllActions.." + maxQValueNextActionAllActions);


        /// we get immediate reward of current action
        Double currentStageReward = getsupervisorsRewardStage ( targetNodeMigration);
        currentStageReward = currentStageReward + ( cpuThreshold - cpuUsage );

        currentStageReward = formateDoubleValue(currentStageReward);



        qValueStateAction =  ( (1-alpha) * (qValueStateAction) )   +  ( alpha* (  currentStageReward + gamma * (  maxQValueNextActionAllActions) ) );

        qValueStateAction = formateDoubleValue(qValueStateAction);

        System.out.println("....updatesupervisorsQvalue... qValueStateAction.." + qValueStateAction);


        /// we update the qvalue matrix with new value
        for ( Entry <String ,  Double > entry : supervisorsQValue.entrySet()) {
            String id_supervisor = "";
            Double qValue = 0.0;
            id_supervisor = entry.getKey();
            qValue = qValueStateAction;
            /// qvalue update
            if (  id_supervisor.equals(targetNodeMigration) ) {

                supervisorsQValue.replace(id_supervisor, qValue);

            }

        }

        System.out.println("....updatesupervisorsQvalue... updated ... final...supervisorsQValue......." + supervisorsQValue);


    }


    public static  Double  getsupervisorsRewardStage ( String supervisorsId)
    {

        Double costFunction = 0.0;
        for ( Entry <String , Map<Boolean, Double> > entry : supervisorsRewardStage.entrySet()) {

            String id_supervisor = "";

            // Double memTotalSupervisor = 0.0;
            Map<Boolean, Double> resources = null;
            id_supervisor = entry.getKey();
            resources = entry.getValue();
            for (Entry <Boolean, Double> entry1 : resources.entrySet())
            {

                if (  id_supervisor.equals(supervisorsId) )
                {
                    costFunction =entry1.getValue();
                }
            }
        }

        System.out.println("....getsupervisorsRewardStage...  costFunction..." + costFunction.toString());


        return costFunction;
    }






    public static  void  updatesupervisorsRewardStage ( )
    {


        System.out.println("....updatesupervisorsRewardStage...before..." + supervisorsRewardStage.toString());

        for ( Entry <String , Map<Boolean, Double> > entry : supervisorsRewardStage.entrySet()) {

            String id_supervisor = "";
            Double cpuFressCapacity = 0.0;
            // Double memTotalSupervisor = 0.0;
            Map<Boolean, Double> resources = null;
            id_supervisor = entry.getKey();
            resources = entry.getValue();
            for (Entry <Boolean, Double> entry1 : resources.entrySet())
            {
               // cpuFressCapacity =entry1.getKey();
                cpuFressCapacity =entry1.getValue();
                boolean isnodeoverutilized = isWorkerNodeOverUtilized( id_supervisor );
                if (  id_supervisor.equals(targetNodeMigration) )
                {

                    System.out.println("....updatesupervisorsRewardStage...id_supervisor.." + id_supervisor);
                    Map<Boolean, Double> visitedCapacity = new HashMap<>();

                    cpuFressCapacity =0.0;   ///
                    //visitedCapacity.put(false, cpuFressCapacity );
                    Double  costFunction = 1 / sourceTopologyCompleteLatencyAfter;
                    costFunction = formateDoubleValue(costFunction);
                    System.out.println("....updatesupervisorsRewardStage...costFunction. after.." + costFunction.toString());


                    visitedCapacity.put(true, costFunction );
                   // supervisorsRewardStage.put ( id_supervisor , visitedCapacity );
                    supervisorsRewardStage.replace ( id_supervisor , visitedCapacity );
                }
            }
        }

        System.out.println("....updatesupervisorsRewardStage......" + supervisorsRewardStage.toString());

    }


    public static  void  initialQvalueMatrix ( )
    {

        for ( Entry <String , Map<Double, Double> > entry : supervisorCapacity.entrySet()) {

            String id_supervisor = "";
            id_supervisor =entry.getKey() ;




            if (  isWorkerNodeSelectedByBenchMark(id_supervisor))
            {

                if (   !id_supervisor.equals(sourceNodeMigration) )
                {
                    supervisorsQValue.put(  id_supervisor , 0.0  );
                    //System.out.println("....initialQvalueMatrix......" + supervisorsQValue.toString());
                }



            }

        }



    }


    public static  void  initialStage ( )
    {


        int cnt = 0;
        int i = 0;
        for (Entry<String, LinkedList<ExecutorDetails>> entry1 : supervisorsExecutersLinkedList.entrySet()) {
            i++;
            String supervisorID = entry1.getKey();
            //System.out.println("....qLearningProcess ..... initialStage......." + supervisorID);
            if (i == 1) sourceNodeMigration = supervisorID;
            //if (i==2)   targetNodeMigration = supervisorID;

        }

        Double executersResourceNeeded = getMigratedExecutersNeededResources();


        for ( Entry <String , Map<Double, Double> > entry : supervisorCapacity.entrySet()) {

            String id_supervisor = "";
            Double cpuFressCapacity = 0.0;
            // Double memTotalSupervisor = 0.0;
            Map<Double, Double> resources = null;
            id_supervisor = entry.getKey();
            resources = entry.getValue();
            for (Entry <Double, Double> entry1 : resources.entrySet())
            {
                cpuFressCapacity =entry1.getKey();
                boolean isnodeoverutilized = isWorkerNodeOverUtilized( id_supervisor );
                if (  cpuFressCapacity >=  executersResourceNeeded && !isnodeoverutilized  &&  !id_supervisor.equals(sourceNodeMigration) )
                {
                    Map<Boolean, Double> visitedCapacity = new HashMap<>();

                    cpuFressCapacity =0.0;   ///
                    //visitedCapacity.put(false, cpuFressCapacity );
                    visitedCapacity.put(false, 0.0 );


                    /// we check whether the supervisors are in the selected list based on the worker nodes


                    if (  isWorkerNodeSelectedByBenchMark(id_supervisor))
                    {
                        supervisorsRewardStage.put ( id_supervisor , visitedCapacity );
                    }

                    System.out.println("....initialStage..... supervisorsRewardStage......." + supervisorsRewardStage);



                }
            }
        }

       // System.out.println("....qLearningProcess ..... supervisorsRewardStage......." + supervisorsRewardStage);


    }


    public static  boolean  isWorkerNodeSelectedByBenchMark ( String id_supervisor)
    {
        boolean found =true; // for testing without bechMarks
        //boolean found = false;

        for ( Entry <String ,Double > entry : sortedCompareWorkernodes.entrySet()) {
            String supervisor = "";
            supervisor = entry.getKey();
            if (supervisor.equals(id_supervisor)){
                found =true;
                System.out.println("....initialREwardStage ..... isWorkerNodeSelectedByBenchMark.....found.." + id_supervisor);
                break;
            }
        }
        return found;
    }


    public static  boolean  isWorkerNodeOverUtilized ( String supervisorId)
    {

      boolean  result=false;
        for (WorkerNodeUsageDetails lst : workerNodeUsageDetailsList) {

            //if    (cpuusage >= cpuUsageSecondThereshold  &&  (!isMigrating)  )



            String SupervisorId = lst.getSupervisorID();
            if (supervisorId.toString().equals(SupervisorId.toString())) {
                Double cpuusage = lst.getCpuUsage();
                if    (cpuusage >= cpuUsageSecondThereshold)
                {
                    result =true;
                    break;
                    //memoryusage = lst.getMemoryUsage();

                }

            }
        }

      return result;

    }


    public static  Double  getMigratedExecutersNeededResources(  ) {

        Double cpuUsage = 0.0;
        Double cpuExecuter = 0.0;

        if (finalTargetComponentMigration.size() == 0) return 0.0;
        for (ExecutorDetails lstexecutersorderItem : finalTargetComponentMigration) {



            ExecutorDetails executerName = null;
            // execToSlotOriginalAssignment.clear() ;

            for (Entry<ExecutorDetails, Map<Double, Double>> entry : executersResources.entrySet()) {


                Map<Double, Double> resourcesNeeded = null;
                executerName = entry.getKey();
                resourcesNeeded = entry.getValue();

                //System.out.println("..executerName....  "  +  executerName );
                if (lstexecutersorderItem.toString().equals(executerName.toString())) {

                    //System.out.println("..getMigratedExecutersResources....  " +    executerName );
                    for (Entry<Double, Double> entry1 : resourcesNeeded.entrySet()) {
                        cpuExecuter += entry1.getKey();

                    }
                }
            }




        }

  return cpuUsage;

    }



    public static  void  assignExecuters(  Topologies topologies, Cluster cluster , List<ExecutorDetails> orderedExecutors) {

      System.out.println(".........assignExecuters.... Executers......" );
        int cnt=0;
        execToSlotOriginalAssignment.clear() ;
        WorkerSlot lastWorkerSlotAcker=null;


        for (ExecutorDetails lstexecutersorderItem : orderedExecutors)
        {

            Double cpuExecuter = 0.0;
            Double memExecuter = 0.0;
            ExecutorDetails executerName = null;
            // execToSlotOriginalAssignment.clear() ;


            for (Entry <ExecutorDetails , Map<Double, Double> > entry : executersResources.entrySet())
            {
               // System.out.println("..assignExecuters..  executersResources..  " +  executersResources );
                // executerName = null;
                Map<Double, Double> resourcesNeeded = null;

                executerName = entry.getKey();
                resourcesNeeded = entry.getValue();

                //System.out.println("..executerName....  "  +  executerName );

                if (lstexecutersorderItem.toString().equals(executerName.toString()))
                {
                    // totalCpu = Double.parseDouble(obj.toString());


                    for (Entry <Double, Double> entry1 : resourcesNeeded.entrySet())
                    {
                        cpuExecuter =entry1.getKey();
                        memExecuter =entry1.getValue();
                    }




                }
            }

            for ( Entry <String , Map<Double, Double> > entry : supervisorCapacity.entrySet()) {
                String id_supervisor = "";
                Double cpuTotalSupervisor =0.0;
                Double memTotalSupervisor =0.0;
                Map<Double, Double> resources = null;

                id_supervisor = entry.getKey();
                resources = entry.getValue();

                for (Entry <Double, Double> entry1 : resources.entrySet())
                {
                    cpuTotalSupervisor =entry1.getKey();
                    memTotalSupervisor =entry1.getValue();
                }

                // System.out.println("..id_supervisor....  " +  id_supervisor +"..cpuTotalSupervisor.."+cpuTotalSupervisor + "..memTotalSupervisor.."+ memTotalSupervisor);

                if (  (cpuExecuter < cpuTotalSupervisor)  &&  (  memExecuter < memTotalSupervisor )   )
                {
                    cnt++;
                    String id_supervisorNew  =id_supervisor  + "-" +cnt;

                    executersToSupervisors.put( id_supervisorNew , lstexecutersorderItem );
                     System.out.println("...Bin packing Assignment..Executer..."  + lstexecutersorderItem +"...To.." + id_supervisorNew);
                    cpuTotalSupervisor -= cpuExecuter;
                    memTotalSupervisor -= memExecuter;

                    /// we update the resources of supervisors
                    resources.clear() ;
                    resources.put(cpuTotalSupervisor, memTotalSupervisor);

                    supervisorCapacity.replace( id_supervisor , resources ) ;

                    WorkerSlot wslot=  SupervisorToWslot( cluster,id_supervisor );
                    lastWorkerSlotAcker = wslot;
                    execToSlotOriginalAssignment.put( lstexecutersorderItem,  wslot );
                   // System.out.println("...Bin packing Assignment..execToSlotOriginalAssignment.."  + execToSlotOriginalAssignment );

                    /////// update bin-Excuters
                    updateSupervisorsExecutersLinkedList(  id_supervisor ,    lstexecutersorderItem );


                    break;

                }

            }




        }



        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
        /// inserting acker executers to assignmet

        ExecutorDetails ackerExecuter =null;

        for (ExecutorDetails  topologyexecuters : topologexecuters)
        {
            if (topologyexecuters.toString().contains("1, 1") )
            {
                execToSlotOriginalAssignment.put( topologyexecuters,  lastWorkerSlotAcker );
                ackerExecuter = topologyexecuters;

                break;
            }
        }
        /////////////////


        /// we update the Reward Matrix for migration
        /// it consists of supervisors and their free capacity
        for ( Entry <String , Map<Double, Double> > entry : supervisorCapacity.entrySet()) {
            String id_supervisor = "";
            Double cpuTotalSupervisor = 0.0;
            Double memTotalSupervisor = 0.0;
            Map<Double, Double> resources = null;

            id_supervisor = entry.getKey();
            resources = entry.getValue();

            for (Entry <Double, Double> entry1 : resources.entrySet())
            {
                cpuTotalSupervisor =entry1.getKey();
                memTotalSupervisor =entry1.getValue();
            }
           // supervisorsRewardFreeCapacity.put ( id_supervisor , cpuTotalSupervisor );
        }





        SchedulerAssignment newAssignment = new SchedulerAssignmentImpl(topologyID, execToSlotOriginalAssignment, slotToResources, null);
        cluster.assign(newAssignment, true);
        System.out.println("....................execToSlotOriginalAssignment..............."  + execToSlotOriginalAssignment);

        execToSlotAfterRebalance.putAll( execToSlotOriginalAssignment );
        System.out.println(".. Bin Packing. assignment is  done. " );
        // logger("..New Bin Packing. assignment is  done... " );

    }








    public static void updateSupervisorsExecutersLinkedList( String id_supervisor ,  ExecutorDetails  lstexecutersorderItem ) {


        for (Entry  <String ,LinkedList< ExecutorDetails> >  supervisorexecuter : supervisorsExecutersLinkedList.entrySet())
        {
            String  idSupervisor = supervisorexecuter.getKey();
            LinkedList<ExecutorDetails> linkedlistExecuters  = supervisorexecuter.getValue();

            if (id_supervisor.equals( idSupervisor))
            {
                linkedlistExecuters.add( lstexecutersorderItem ) ;
                supervisorsExecutersLinkedList.put( idSupervisor , linkedlistExecuters );
               // System.out.println("......after  update.............supervisorsExecutersLinkedList.put...................." +supervisorsExecutersLinkedList);
                break;
            }
        }



    }



    public static String setSupervisorMigrateTest( Cluster cluster, String id_supervisor)
    {
        String supervisorId="";

           Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
           for (Entry<String, SupervisorDetails> entry : supervisordetls.entrySet()) {

            supervisorId = entry.getKey();
            SupervisorDetails supdetails = entry.getValue();
            String supervisorid = supdetails.getId();
            if (supervisorid.contains(id_supervisor)) {
                targetNodeMigration = supervisorid;
            }
            ///test
        }

        //System.out.println("........setSupervisorMigrateTest.... "+ targetNodeMigration );
           return targetNodeMigration;

    }


        public static WorkerSlot SupervisorToWslot( Cluster cluster ,String id_supervisor)
    {
        WorkerSlot result=null;


        //WorkerSlot avilabslots = null;
       // List<WorkerSlot>   avilabslots = cluster.getAssignableSlots();
        List<WorkerSlot>   avilabslots = cluster.getAvailableSlots();
       // System.out.println("..SupervisorToWslot. id_supervisor "+ id_supervisor  );
       // System.out.println("..SupervisorToWslot. avilabslots "+ avilabslots  );

        for (WorkerSlot slot : avilabslots) {
            // System.out.println("..Ant Colony.scedule. workerslot "+ port  );

             if (slot.toString().contains(id_supervisor) )
             {
                 result = slot;
                // System.out.println("........Return ..slot.... "+ result );
                 break;
             }

        }
        return result;

    }




    public static List<ExecutorDetails> orderExecutors(
            TopologyDetails td, Collection<ExecutorDetails> unassignedExecutors) {
        Map<String, Component> componentMap = td.getComponents();

        //td.getTopology().

        List<ExecutorDetails> execsScheduled = new LinkedList<>();

        Map<String, Queue<ExecutorDetails>> compToExecsToSchedule = new HashMap<>();
        for (Component component : componentMap.values()) {

            //component.getExecs().

            //System.out.println("...Bin Packing......orderExecutors..1.orderExecutors.  "  + component.toString()  );
            compToExecsToSchedule.put(component.getId(), new LinkedList<ExecutorDetails>());
            for (ExecutorDetails exec : component.getExecs()) {
                if (unassignedExecutors.contains(exec)) {
                    compToExecsToSchedule.get(component.getId()).add(exec);
                    //System.out.println("...Bin Packing...orderExecutors.2..compToExecsToSchedule...  "  + compToExecsToSchedule.toString()  );

                }
            }
        }

        System.out.println("..........................1..............................  "   );

        Set<Component> sortedComponents = sortComponents(componentMap);


       System.out.println("..........................2..............................  "   );
       System.out.println("...Bin Packing...sortedComponents.1..full...  "  + sortedComponents.toString()  );
       // System.out.println("..........................3..............................  "   );
        sortedComponents.addAll(componentMap.values());
        System.out.println("...Bin Packing...sortedComponents..2.full...  "  + sortedComponents.toString()  );

        for (Component currComp : sortedComponents) {
            Map<String, Component> neighbors = new HashMap<String, Component>();
            for (String compId : Sets.union(currComp.getChildren(), currComp.getParents())) {
                neighbors.put(compId, componentMap.get(compId));
                System.out.println("...Bin Packing...neighbors...  "  + neighbors  );
            }
            Set<Component> sortedNeighbors = sortNeighbors(currComp, neighbors);
            System.out.println("...Bin Packing...sortNeighbors..full.  "  + sortedNeighbors  );
            Queue<ExecutorDetails> currCompExesToSched = compToExecsToSchedule.get(currComp.getId());

            boolean flag = false;
            do {
                flag = false;
                if (!currCompExesToSched.isEmpty()) {
                    execsScheduled.add(currCompExesToSched.poll());
                    System.out.println("...Bin Packing...execsScheduled.  "  + execsScheduled  );
                    System.out.println("...Bin Packing...execsScheduled.  "  + currCompExesToSched  );
                    flag = true;
                }

                for (Component neighborComp : sortedNeighbors) {
                    Queue<ExecutorDetails> neighborCompExesToSched = compToExecsToSchedule.get(neighborComp.getId());
                    if (!neighborCompExesToSched.isEmpty()) {
                        execsScheduled.add(neighborCompExesToSched.poll());
                        System.out.println("...Bin Packing...execsScheduled.  "  + execsScheduled  );

                        flag = true;
                    }
                }
            } while (flag);
        }
        return execsScheduled;
    }




   public static  Set<Component> sortComponents(final Map<String, Component> componentMap) {
        Set<Component> sortedComponents =
                new TreeSet<>((o1, o2) -> {
                    int connections1 = 0;
                    int connections2 = 0;
                    System.out.println("...Bin Packing...Total "  + componentMap  );

                    for (String childId : Sets.union(o1.getChildren(), o1.getParents())) {

                        System.out.println(".......************************...... "    );
                        System.out.println(".......************************...... "    );
                        System.out.println("...Bin Packing...sortComponents..1. o1.getChildren() "  + o1  );
                          System.out.println("...Bin Packing...sortComponents..1. o1.getChildren() "  + o1.getChildren()  );
                        System.out.println("...Bin Packing...sortComponents...1 o1.getParents() "  + o1.getParents()  );
                        connections1 += (componentMap.get(childId).getExecs().size() * o1.getExecs().size());

                        System.out.println("...Bin Packing...sortComponents..1. componentMap get(childId)"  + componentMap.get(childId)  );
                        System.out.println("...Bin Packing...sortComponents..1. componentMap.get(childId).getExecs().size()"  + componentMap.get(childId).getExecs().size()  );
                       System.out.println("...Bin Packing...sortComponents..1. o1.getExecs().size()"  + o1.getExecs().size()  );

                       System.out.println("...Bin Packing...sortComponents..1. connections1 "  + connections1  );
                    }

                    for (String childId : Sets.union(o2.getChildren(), o2.getParents())) {

                        System.out.println("...Bin Packing...sortComponents..2. o1.getChildren() "  + o2  );
                        System.out.println("...Bin Packing...sortComponents..2. o1.getChildren() "  + o2.getChildren()  );
                        System.out.println("...Bin Packing...sortComponents..2 o1.getParents() "  + o2.getParents()  );

                        connections2 += (componentMap.get(childId).getExecs().size() * o2.getExecs().size());

                        System.out.println("...Bin Packing...sortComponents..2. componentMap get(childId)"  + componentMap.get(childId)  );
                        System.out.println("...Bin Packing...sortComponents..2. componentMap.get(childId).getExecs().size()"  + componentMap.get(childId).getExecs().size()  );
                        System.out.println("...Bin Packing...sortComponents..2. o1.getExecs().size()"  + o2.getExecs().size()  );

                        System.out.println("...Bin Packing...sortComponents..2. connections1 "  + connections2  );
                        System.out.println("...Bin Packing...Total values"  + componentMap  );

                    }

                    if (connections1 > connections2) {
                        return -1;
                    } else if (connections1 < connections2) {
                        return 1;
                    } else {
                        System.out.println("...Bin Packing...Tota Compare "  + o1.getId().compareTo(o2.getId())  );
                        return o1.getId().compareTo(o2.getId());

                    }
                });
        sortedComponents.addAll(componentMap.values());
       System.out.println("...Bin Packing...Total "  + componentMap  );
       System.out.println("...Bin Packing...sortedComponents "  + sortedComponents  );
        return sortedComponents;
    }


    public static Set<Component> sortNeighbors(
            final Component thisComp, final Map<String, Component> componentMap) {
        Set<Component> sortedComponents =
                new TreeSet<>((o1, o2) -> {
                    int connections1 = o1.getExecs().size() * thisComp.getExecs().size();
                    int connections2 = o2.getExecs().size() * thisComp.getExecs().size();
                    if (connections1 < connections2) {
                        return -1;
                    } else if (connections1 > connections2) {
                        return 1;
                    } else {
                        return o1.getId().compareTo(o2.getId());
                    }
                });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }




    public static  void  checkWorkernodesThershold( List<WorkerNodeUsageDetails> workerNodeUsageDetailsList,List <ComponentCPUMemoryInfo> listcomponentCPUMemoryInfo , Topologies topologies, Cluster cluster) {

    // Analayze

        String SupervisorId = "";
        Double cpuusage = 0.0;
        Double memoryusage = 0.0;


        if (!isBenchMarkeTasksKilled) {
            supervisorsBenchmarkExecute.putAll(BenchMarkGetInfo.supervisorsBenchmarkExecute);
        }


        if (epochNumber == 1) {
            System.out.println("...........checkWorkernodesThershold........supervisorsBenchmarkExecute.........." + supervisorsBenchmarkExecute);
        }
        int i=0;



       // boolean a= true; if(a) return;

        for (Entry<String, LinkedList<ExecutorDetails>> entry1 : supervisorsExecutersLinkedList.entrySet())
        {
            i++;
            String supervisorID =entry1.getKey();
            if (i==1)
            {
                boolean  isInTabooList = isWorkerNodeInTabooList(supervisorID);
                if (!isInTabooList) {
                    sourceNodeMigration = supervisorID;
                }else
                {
                    System.out.println("...........checkWorkernodesThershold........is in TabooList......" + sourceNodeMigration  );
                }
            }
        }



        Long currentTopolgyInput ;



        if (!ongoingMigrationProcess && !ongoingReverseMigrationProcess)
        {
            currentTopolgyInput =  getTopologyInputRate(   topologyID );

        }else
        {
            currentTopolgyInput =0L;
            inputRateTopology =0L;
            previousTupleEmitted=0L;
        }


        originalAssignment = cluster.getAssignmentById(topologyID);

        //System.out.println("...........checkWorkernodesThershold........originalAssignment.........." + originalAssignment.si );


        if (  currentTopolgyInput > 0 )
        {

            //getTopologyInfoLogger(  cluster,  topologies );
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Calendar cal = Calendar.getInstance();
            dateFormat.format(cal.getTime());

            System.out.println("...Time..."+ dateFormat.format(cal.getTime()) + "...Total  Emitted...." + currentTopolgyInput +"..Input Rate..." + inputRateTopology);

//            System.out.println("............currentTopolgyInput..."+ currentTopolgyInput );
//            System.out.println("............previousTupleEmitted..."+ previousTupleEmitted );

            inputRateTopology =   (currentTopolgyInput - previousTupleEmitted ) ;////10 ;
            System.out.println("............inputRateTopology..."+ inputRateTopology );
            System.out.println("............Number of Migrations..."+ MigrationNumber );

            previousTupleEmitted = currentTopolgyInput;





            // System.out.println("..........................................................." );
        }

        cpuThreshold = cpuUsageTheresholdReturn;

            for (WorkerNodeUsageDetails lst : workerNodeUsageDetailsList) {

                SupervisorId = lst.getSupervisorID();
                cpuusage = lst.getCpuUsage();
                memoryusage = lst.getMemoryUsage();
                LinkedHashMap<String, Double> sortedCompare =  new LinkedHashMap<>();
             //   System.out.println(".............SupervisorId.........CPUusage........MemoryAvailable....."+SupervisorId+":::"+cpuusage +":::"+memoryusage );
              //   benchMarckCpuLoadLogger(SupervisorId, supervisorsBenchmarkExecute, workerNodeUsageDetailsList);

                if (isMigrating)
                {   /// we check if we are migrating whether the source node thereshold is under the thereshold




                }

               // System.out.println("......checkWorkernodesThershold....111..." );
                if (!isMigrating) {
                    // if    (cpuusage >= cpuUsageSecondThereshold  &&  (!isMigrating)  ) {
                    /// TheresholdInputRateMigration =15000 at first step
                    if (  currentTopolgyInput > TheresholdInputRateMigration  ) {    /// in real test we check inputrate or we can add cpuusage
//                   if (  inputRateTopology > landa  ) {



                            System.out.println(".........................Migration is Starting...................................." );
                             sourceNodeMigration = SupervisorId;
                            // by this function we check whether  the load of each node is real or not and is generated by bolt component or storm misellanios
                            boolean result = checkOverLoadIsByRealComponent(  cluster,  topologies , listcomponentCPUMemoryInfo);
                            if (!result){
                                sourceNodeMigration="";
                                //System.out.println("..................................fake load .............." );
                                return;
                            }

                            sortedCompareWorkernodes.clear();
                            System.out.println("................................ sourceNodeMigration.........." + sourceNodeMigration);

                            sortedCompare = selectTargetNode(sourceNodeMigration, supervisorsBenchmarkExecute, workerNodeUsageDetailsList);

                            sortedCompare = setPriority(adjSourceworker,  adjtargetworker,  cpuUsage ,  cpuThreshold, sortedCompare);


                            sortedCompareWorkernodes.putAll(sortedCompare);
                            System.out.println(".................................sortedCompare.........." + sortedCompare);

                          //  System.out.println(".................................targetNodeMigration.........." + targetNodeMigration);
                            testcounter = 0;
                            isMigrating = true;

                            inputRateTopologyMigration =inputRateTopology ;




                    }



                }


            }


       // }

    }






    public static Boolean checkOverLoadIsByRealComponent( Cluster cluster, Topologies topologies ,List <ComponentCPUMemoryInfo> listcomponentCPUMemoryInfo)
    {


        Boolean result =false;

        Map<String, String> selectedComponent =  new HashMap<>();
        Map <String , Map <String , Map<Double, Double>>>  topologyCompponentsInfo  =  new HashMap<>();
        topologyCompponentsInfo =GetExecuterDetails.getExecuterDetailsDaemon();
        List <ExecuterMetricsDetails>  listexecuterTotalMetricsDetails = new  ArrayList<ExecuterMetricsDetails>();
        listexecuterTotalMetricsDetails = createTotalListComponentsInfo(topologyCompponentsInfo,listcomponentCPUMemoryInfo, cluster );


        //System.out.println("......checkOverLoadIsByRealComponent..result.......result  " +result );
        return result;
    }

    public static void performMigration( Cluster cluster, Topologies topologies ,List <ComponentCPUMemoryInfo> listcomponentCPUMemoryInfo, boolean allowMigration)
    {

       /// Plan && Execute

        Map<String, String> selectedComponent =  new HashMap<>();
        Map <String , Map <String , Map<Double, Double>>>  topologyCompponentsInfo  =  new HashMap<>();
        topologyCompponentsInfo =GetExecuterDetails.getExecuterDetailsDaemon();
        List <ExecuterMetricsDetails>  listexecuterTotalMetricsDetails = new  ArrayList<ExecuterMetricsDetails>();
        listexecuterTotalMetricsDetails = createTotalListComponentsInfo(topologyCompponentsInfo,listcomponentCPUMemoryInfo, cluster );

        //getTopologyInfoLogger(  cluster,  topologies );


       //  displayListComponent(listexecuterTotalMetricsDetails);

        //if (isMigrating) return;
        if (!isMigrating) return;
        if (listexecuterTotalMetricsDetails.size()> 0) {
            testcounter++;
            WorkerSlot targetWorkerslot = null;
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            for (WorkerSlot port : availableSlots) {
                if (port.toString().contains(targetNodeMigration)) {
                    targetWorkerslot = port;
                }
            }


            String sourcetopologyID = "";
            String sourcetopologyName = "";
            String sourceComponent = "";



            if (isMigrating  &&  !FirstRun ) {
                ExecuterMetricsDetails finalTargetComponentReplication = selectComponentForReplication(listexecuterTotalMetricsDetails, cluster, topologies);
                System.out.println("........finalTargetComponentReplication.......  " + finalTargetComponentReplication.getComponent());


                if (finalTargetComponentReplication == null) {
                    System.out.println("........No componet is found for Replication.......  ");
                    return;
                }





                //System.out.println("........perform       3.......  " );
                finalTargetComponentMigration= selectComponentForMigration(   listexecuterTotalMetricsDetails  ,  finalTargetComponentReplication ,cluster,  topologies );
                System.out.println("........finalTargetComponentMigration.......  " +  finalTargetComponentMigration );

                //if (finalTargetComponentMigration ==null){
                if (finalTargetComponentMigration == null) {
                    System.out.println("........No componet is found to migratetion.......  ");
                    return;
                }




                String topologyId = "";
                //int availablePort= port.;


                //if (finalTargetComponentMigration ==null){
                if (finalTargetComponentReplication != null) {
                    sourcetopologyID = finalTargetComponentReplication.getTopologyID();

                    // sourcetopologyID = topologyId;
                    String[] arrSplit = sourcetopologyID.split("-");
                    sourcetopologyName = arrSplit[0];
                    topologyName = sourcetopologyName;
                    sourceComponent = finalTargetComponentReplication.getComponent();
                    //sourceComponent = finalTargetComponent;
                    System.out.println(".... performMigration......SelectedComponet ......" + sourcetopologyName + "..." + sourceComponent);
                }


                TheresholdprobabilityToMigrate =80.0;  /// for Test
                Long InputRateRange = inputRateTopologyMigration/1000;
                InputRateRange =5L;

                Double probabityprediction   =   MachineLearningScheduler.BayesClassifyerInputRate(    InputRateRange,  sourceComponent);
                System.out.println(".... performMigration......probabityprediction ......" + probabityprediction );
                if  (  probabityprediction < TheresholdprobabilityToMigrate)   /// in this case we do not migrate
                {
//                    isMigrating =false;
//                    testcounter=0;
//                    FirstRun =false;
//                    return;
                }


            }


            //boolean a=true; if (a) return;

            Double sourceCapacity = listexecuterTotalMetricsDetails.get(0).getCapacity() ;
            Double sourceCpuUsage = listexecuterTotalMetricsDetails.get(0).getComponentCPUUsage() ;
            Double sourceMemoryUsage = listexecuterTotalMetricsDetails.get(0).getComponentMemoryUsage() ;



            allowMigration =true;


            WorkerSlot sourceWorkerSlot =null;
            List<ExecutorDetails> executorToAssignOnTotal=null;
            Map<WorkerSlot ,List<ExecutorDetails>> resultMigration = new HashMap<>();
            System.out.println(".................testcounter....................."+testcounter);


            //Double  sourceTopologyCompleteLatencyBefore=0.0;
            Double  targetTopologyCompleteLatencyBefore=0.0;
            Double  targetWorkerNoneCpuLoad=0.0;
            Double  sourceWorkerNoneCpuLoad =0.0;
            String targetTopologyId="";

           // System.out.println("........performMigration....5...  ");
            if (!FirstRun &&  testcounter >= 1  && allowMigration) {


                isMigrating =true;
                FirstRun = true;
                sourceTopologyCompleteLatencyBefore = getTopologyCompleteTime(topologyID);
                System.out.println("...........perform migration..........sourceTopologyCompleteLatencyBefore........" + sourceTopologyCompleteLatencyBefore);

               // System.out.println("...........perform migration..........111........" );
                if (!targetTopologyId.equals("") ) {
                   // targetTopologyCompleteLatencyBefore = getTopologyCompleteTime(targetTopologyId);
                }
                else{
                    targetTopologyCompleteLatencyBefore = 0.0;
                }


              //  updateAssignmentMigration ( cluster,  finalTargetComponentMigration);
              //  originalAssignment = cluster.getAssignmentById(sourcetopologyID);
              //  System.out.println(".......................performMigration......updateAssignmentMigration........"+originalAssignment );
                originalExecsBeforeRebalance = getComponentExecuters(cluster, sourceComponent.toString() ) ;
                System.out.println("...........perform migration........originalExecsBeforeRebalance.........." + originalExecsBeforeRebalance );
                componentNameMigrated = sourceComponent;
                ongoingMigrationProcess =true;

                initialQlearning();
               // boolean a= true; if(a) return;

                //MigrationScheduler.rebalanceCompoent( sourcetopologyName , sourceComponent  ,  parallelizationDegree )  ;

                originalReplicatedExecuter = getComponentExecuters(cluster, sourceComponent.toString() ) ;  /// it is only used when we want to use migration withou RSR method
                //System.out.println("...........perform migration........originalReplicatedExecuter....." + originalReplicatedExecuter );
                MigrationScheduler.rebalanceComponent( topologyName, sourceComponent  ,  parallelizationDegree );

                 previousTupleEmitted=0L;
                 testcounterMigration =0;
                rebalaceIsDone = true;


            }

            testcounterMigration ++;
            System.out.println("...........perform migration........testcounterMigration....." + testcounterMigration );

            boolean allowToMigrateToNewWorkerNode =false;

            /// in this case if the previous migration is assessed by Qlearning process and still is in rebalance process
            /// if the incoming work load increases al least 2*landa then we migrate to the new worker node instead od reverse migration

            boolean  movefromotherworkernode= false;
            if ( rebalaceIsDone    &&  testcounterMigration > 4 )  /// 3
            {
                System.out.println("...........inside if .rebalaceIsDone    &&  testcounterMigration." + allowToMigrateToNewWorkerNode );
                // if (  inputRateTopology > 2* landa )  /// for real test
                if ( turnToMoveToOtherWorkerNode ==0  )
                {
//                    allowToMigrateToNewWorkerNode =true;
//                    turnToMoveToOtherWorkerNode++;
//                    movefromotherworkernode =true;

                }
            }


              System.out.println("...........allowToMigrateToNewWorkerNode.." + allowToMigrateToNewWorkerNode );
              if (  (rebalaceIsDone    && testcounterMigration ==2)  ||  (  allowToMigrateToNewWorkerNode)    )
                //if (rebalaceIsDone    && (testcounterMigration ==2 || inputtopologyrate > 2* landa))
            {


                System.out.println("....supervisorsRewardStage......." + supervisorsRewardStage);
                System.out.println("....supervisorsQValue......" + supervisorsQValue.toString());

                previousTupleEmitted =0L;
                newAddedExecsAfterRebalance = getComponentExecuters(cluster, sourceComponent.toString() ) ;
                String supervisorid="";
                //supervisorQvalueLogger( );

                if (   epochNumber >= maxEpochNumberConvergannce ){ /// if we reach to convergence


                    //supervisorQvalueLogger( );
//                    Random rand = new Random();
//                    float float_random=rand.nextFloat();
                    System.out.println("...........epochNumber   is higher than thershole ...epochNumber......" + epochNumber  );

                    Random rand = new Random();
                    int max=10;
                    int min =1;
                    int randomNum = rand.nextInt((max - min) + 1) + min;
                    System.out.println("...................randomNum.........." + randomNum  );
                    /// here the real value of epsilon is 0.2

                    if ( randomNum < epsilon)   /// here we should select between exploration and exploitation
                    {
                        supervisorid = selectAction( movefromotherworkernode) ;    /// exploration
                        System.out.println("...................exploratiion.........."   );

                    }else
                    {
                        supervisorid = getBestsupervisorsQvalue();                 /// exploitation
                        System.out.println("...................exploitation.........."   );
                    }




                }else
                {
                     supervisorid = selectAction( movefromotherworkernode) ;
                }
                   //

                if ( !supervisorid.equals("")) {

                    targetNodeMigration = supervisorid;



                }




                System.out.println("........targetNodeMigration...."+targetNodeMigration );

                if (  allowToMigrateToNewWorkerNode )
                {
                    System.out.println(".....Inside..If....MoveTOotherWorkerNode..................");
                    System.out.println("........targetNodeMigration...."+targetNodeMigration );
                    turnToMoveToOtherWorkerNode++;
                    updateAssignmentMigrationToNewNewNode ( cluster , newAddedExecsAfterRebalance  );
                    testcounterMigration =2;

                    turnToMoveToOtherWorkerNode++;

                    allowToMigrateToNewWorkerNode =false;
                    return;


                }else {

                  updateAssignmentMigrationRSR(cluster, finalTargetComponentMigration, newAddedExecsAfterRebalance, originalExecsBeforeRebalance);
                  // updateAssignmentMigrationWithoutRSR(cluster, finalTargetComponentMigration, newAddedExecsAfterRebalance, originalExecsBeforeRebalance);

                }
                //updateAssignmentMigrationToNewNewNode ( cluster , newAddedExecsAfterRebalance  );


                //turnToMoveToOtherWorkerNode++;   /// only for test to change the input rate
                previousTupleEmitted=0L;
                System.out.println("......... epochNumber......." + epochNumber);
                long start = System.currentTimeMillis();
                System.out.println(".....Migration..is done........" );


                   Long InputRateRange = inputRateTopologyMigration/1000;   /// here we convert inputRate into a range

                  //System.out.println("............InputRateRange Int ..."+ InputRateRange );

                    ElasticityComponentInfo elasticityComponentInfo =new ElasticityComponentInfo();
                    elasticityComponentInfo.setsourceSuperviorID(listexecuterTotalMetricsDetails.get(0).getSuperviorID());
                    elasticityComponentInfo.setSourceSuperviorName(listexecuterTotalMetricsDetails.get(0).getSuperviorID());
                    elasticityComponentInfo.setTargetSuperviorIDSuperviorID(targetNodeMigration);
                    elasticityComponentInfo.setTargetSuperviorName(targetNodeMigration);
                    elasticityComponentInfo.setTopologyID(sourcetopologyID);   /// topologyName
                    elasticityComponentInfo.setTopologyName(topologyName);
                    elasticityComponentInfo.setInputRateTopology(InputRateRange);
                    elasticityComponentInfo.setComponent(sourceComponent);
                    elasticityComponentInfo.setSourceWorkerSlot(sourceWorkerSlot) ;
                    elasticityComponentInfo.setTargetWorkerSlot(targetWorkerslot);
                    elasticityComponentInfo.setExecutorToAssignOnTotal(executorToAssignOnTotal);
                    elasticityComponentInfo.setTargetWorkerNoneCpuLoad(targetWorkerNoneCpuLoad) ;
                    elasticityComponentInfo.setSourceWorkerNoneCpuLoad(sourceWorkerNoneCpuLoad);
                    elasticityComponentInfo.setSourceCapacity(sourceCapacity);
                    elasticityComponentInfo.setSourceCPUUsage(sourceCpuUsage);
                    elasticityComponentInfo.setSourceMemoryUsage(sourceMemoryUsage);
                    elasticityComponentInfo.setTargetTopologyID(targetTopologyId) ;
                    elasticityComponentInfo.setSourceTopologyCompleteLatencyBefore(sourceTopologyCompleteLatencyBefore) ;
                    elasticityComponentInfo.setTargetTopologyCompleteLatencyBefore(targetTopologyCompleteLatencyBefore);
                    elasticityComponentInfo.setReturned(false);
                    elasticityComponentInfo.setStartTime(start);
                    elasticityComponentInfo.setEndTime(0);
                    elasticityComponentInfosList.clear();
                    elasticityComponentInfosList.add(elasticityComponentInfo);

            }

            if (rebalaceIsDone    &&   testcounterMigration ==3)   /// In this step we evalute the actions in Qlearningprocess
            {

                System.out.println("...........perform migration........saveAndEvaluateevaluateAction.........."  );
                sourceTopologyCompleteLatencyAfter =  getTopologyCompleteTime(topologyID);
                sourceTopologyCompleteLatencyAfter =sourceTopologyCompleteLatencyAfter /1000;
                BigDecimal bigDecimal = new BigDecimal(String.valueOf(sourceTopologyCompleteLatencyAfter));
                int intValue = bigDecimal.intValue();
                System.out.println("...........perform migration....after.............." + sourceTopologyCompleteLatencyAfter );
                qLearningProcess();
                supervisorQvalueLogger( );
                //turnToMoveToOtherWorkerNode++;
            }

        }

        //return selectedComponent;
    }

    public static  List< ExecutorDetails > getComponentExecuters (Cluster cluster , String componentName)
    {

        originalAssignment = cluster.getAssignmentById(topologyID );
        List< ExecutorDetails > listExcutersDetails = new ArrayList< ExecutorDetails>();

        Map<String, Component> componentMap = glbtopologyDetailes.getComponents();

        for (Component component : componentMap.values()) {

            listExcutersDetails =component.getExecs();
            if (component.toString().equals( componentName.toString() ) )
            {
                break;
            }

        }

        return listExcutersDetails;
    }



    public static void updateAssignmentReverseMigration (Cluster cluster )
    {

        cluster.unassign(topologyID);
        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
        SchedulerAssignment newAssignment = new SchedulerAssignmentImpl(topologyID, execToSlotOriginalAssignment, slotToResources, null);
        cluster.assign(newAssignment, true);

        //System.out.println("...........updateAssignmentReverseMigration..........execToSlotOriginalAssignment......" + execToSlotOriginalAssignment);
        System.out.println("...........Reverese migration is done................");
    }


    public static boolean isWorkerNodeInTabooList(String sourceworkernode)
    {

        boolean found =false;

        for (String  workernode : tabooList) {


            //String worker =
            if (sourceworkernode.equals(workernode))
            {
                found =true;
                break;
            }


        }

        return found;
    }

    public static String updateAssignmentPermanent(Cluster cluster)
    {


        System.out.println(".updateAssignmentPermanent............." );
        String id_supervisorSelected ="";
        int cnt=0;
        Double firstResponseTime =0.0;
        int foundBestResult =0;



        for (ActionEvaluation actionEvaluation : lstActionEvaluation) {

            List<ExecutorDetails>   executerDetails =actionEvaluation.executerDetails;
            String  sourceSupervisorID = actionEvaluation.sourceSupervisorID ;
            String  targetSupervisorID =actionEvaluation.targetSupervisorID;
            Double costValueBeforMigration =actionEvaluation.costValueBeforMigration;
            Double costValueAfterMigration =actionEvaluation.costValueAfterMigration;
            //actionEvaluation.

            int epochnumber = actionEvaluation.epochnumber;
            Long inputRate = actionEvaluation.inputRate;

            if (cnt == 0)
            {
                firstResponseTime =  costValueBeforMigration;
            }else
            {
                if(   costValueAfterMigration < firstResponseTime)
                {
                    firstResponseTime =  costValueAfterMigration ;
                    foundBestResult =cnt;
                }
            }



            System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......cnt.." + cnt);
            System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......executerDetails.." + executerDetails);
            System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......sourceSupervisorID.." + sourceSupervisorID);
            System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......targetSupervisorID.." + targetSupervisorID);
            System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......costValueBeforMigration.." + costValueBeforMigration);
            System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......costValueAfterMigration.." + costValueAfterMigration);
            System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......epochnumber.." + epochnumber);
            System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......inputRate.." + inputRate);
            System.out.println("........................................................................." );

            cnt++;



        }


        if ( foundBestResult >0 ) {   /// when we find a evalution track that has response time better than relocation
            ActionEvaluation permanetScheduling = lstActionEvaluation.get(cnt);
            execToSlotAfterRebalance.putAll( permanetScheduling.execToSlotAfterRebalanceEvaluation);
            id_supervisorSelected = permanetScheduling.targetSupervisorID;
        }

        System.out.println(".updateAssignmentPermanent.getBestMigrationValue.......firstResponseTime.." + firstResponseTime);


        cluster.unassign(topologyID);
        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
        SchedulerAssignment newAssignment = new SchedulerAssignmentImpl(topologyID, execToSlotAfterRebalance, slotToResources, null);
        cluster.assign(newAssignment, true);
        System.out.println(".updateAssignmentPermanent........permananet scheduling has happened.." );
        // System.out.println(".......................performMigration...updateAssignmentMigrationToNewNewNode...2........" );
        ongoingMigrationProcess = false;

        return id_supervisorSelected;

    }



    public static void updateAssignmentMigrationToNewNewNode(Cluster cluster,  List<ExecutorDetails> newAddedExecsAfterRebalance    )
    {

        for (  Entry <ExecutorDetails, WorkerSlot>  entry : execToSlotAfterRebalance.entrySet()) {
            ExecutorDetails executerName = entry.getKey();
            WorkerSlot wslot = entry.getValue();
            for (  ExecutorDetails  newExecuter :   newAddedExecsAfterRebalance)
            {
                WorkerSlot wslotnew = SupervisorToWslot( cluster,targetNodeMigration );
                execToSlotAfterRebalance.replace(  newExecuter , wslotnew   );
                // break;
            }
        }

       //System.out.println(".......................performMigration...updateAssignmentMigrationToNewNewNode...execToSlotOriginalAssignment.....after..." + execToSlotAfterRebalance );
        cluster.unassign(topologyID);
        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
        SchedulerAssignment newAssignment = new SchedulerAssignmentImpl(topologyID, execToSlotAfterRebalance, slotToResources, null);
        cluster.assign(newAssignment, true);
       // System.out.println(".......................performMigration...updateAssignmentMigrationToNewNewNode...2........" );
        ongoingMigrationProcess = false;


    }


    public static void updateAssignmentMigrationWithoutRSR (Cluster cluster,  List <ExecutorDetails> finalTargetComponentMigration , List<ExecutorDetails> newAddedExecsAfterRebalance  , List<ExecutorDetails> originalExecsBeforeRebalance   ) {


        finalTargetComponentMigration.clear();
        finalTargetComponentMigration.addAll(newAddedExecsAfterRebalance);
        /// removing the old executer before replicated
         Map<ExecutorDetails, WorkerSlot> tempexecToSlotOriginalAssignment = new HashMap<>();
        for (  Entry <ExecutorDetails, WorkerSlot>  entry : execToSlotOriginalAssignment.entrySet()) {
            // executerName = null;

            Map<Double, Double> resourcesNeeded = null;
            ExecutorDetails executerName = entry.getKey();
            WorkerSlot wslot = entry.getValue();
            /// remove  old executer of selected componet for replication
            for (ExecutorDetails originalExecuter : originalReplicatedExecuter) {
                if (!executerName.toString().equals(originalExecuter.toString())) {
                    tempexecToSlotOriginalAssignment.put(executerName, wslot);
                }
            }
        }


          /// we add new replicated executers
            int i=0;
          WorkerSlot wslottarget;
            for (ExecutorDetails executersselected : finalTargetComponentMigration) {
                if (i==1) {
                     wslottarget = SupervisorToWslot(cluster, targetNodeMigration);
                }else
                {
                     wslottarget = SupervisorToWslot(cluster, sourceNodeMigration);
                }
                 tempexecToSlotOriginalAssignment.put(executersselected, wslottarget);
                i++;
            }
        execToSlotAfterRebalance.clear() ;
        execToSlotAfterRebalance.putAll( tempexecToSlotOriginalAssignment) ;
        System.out.println(".......................updateAssignmentMigrationWithoutRSR....after adding replicated executers..execToSlotAfterRebalance........"+execToSlotAfterRebalance );

        cluster.unassign(topologyID);
        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();

        SchedulerAssignment newAssignment = new SchedulerAssignmentImpl(topologyID, execToSlotAfterRebalance, slotToResources, null);
        cluster.assign(newAssignment, true);
        ongoingMigrationProcess = false;
        //System.out.println(".....Migration..is done........" );




    }



        public static void updateAssignmentMigrationRSR (Cluster cluster,  List <ExecutorDetails> finalTargetComponentMigration , List<ExecutorDetails> newAddedExecsAfterRebalance  , List<ExecutorDetails> originalExecsBeforeRebalance   )
    {
        /// removing the old executer before replicated

        Map<ExecutorDetails, WorkerSlot> tempexecToSlotOriginalAssignment = new HashMap<>();


        for (  Entry <ExecutorDetails, WorkerSlot>  entry : execToSlotOriginalAssignment.entrySet()) {
            // executerName = null;

            Map<Double, Double> resourcesNeeded = null;
            ExecutorDetails executerName = entry.getKey();
            WorkerSlot wslot = entry.getValue();
            /// remove  old executer of selected componet for replication
            for (ExecutorDetails originalExecuter : originalReplicatedExecuter) {
                if (!executerName.toString().equals(originalExecuter.toString())) {
                    tempexecToSlotOriginalAssignment.put(executerName, wslot);
                }
            }
        }


        //System.out.println(".......................updateAssignmentMigration......tempexecToSlotOriginalAssignment..after removing original executer......"+tempexecToSlotOriginalAssignment );


        for (  Entry <ExecutorDetails, WorkerSlot>  entry : execToSlotOriginalAssignment.entrySet()) {
         //executerName = null;

        /// replace the workslot of selected component for migration


                for (ExecutorDetails executersselected : finalTargetComponentMigration) {

                     ExecutorDetails executerName = entry.getKey();
                     WorkerSlot wslot = entry.getValue();

                      if (executerName.toString().equals(executersselected.toString())) {
                      WorkerSlot wslottarget = SupervisorToWslot(cluster, targetNodeMigration);
                       tempexecToSlotOriginalAssignment.replace(executersselected, wslottarget);
                       break;
                     }

                }


         }

        //System.out.println(".......................updateAssignmentMigration....after replacing moved executers..tempexecToSlotOriginalAssignment........"+tempexecToSlotOriginalAssignment );




        /// add  new executers of selected componet after replication
        for (  ExecutorDetails  newExecuter :   newAddedExecsAfterRebalance)
        {

            WorkerSlot wslot = SupervisorToWslot( cluster,sourceNodeMigration );
            tempexecToSlotOriginalAssignment.put(  newExecuter , wslot   );

        }


        execToSlotAfterRebalance.clear() ;
        execToSlotAfterRebalance.putAll( tempexecToSlotOriginalAssignment) ;
        //System.out.println(".......................updateAssignmentMigration....after adding replicated executers..execToSlotAfterRebalance........"+execToSlotAfterRebalance );



        cluster.unassign(topologyID);
        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();

        SchedulerAssignment newAssignment = new SchedulerAssignmentImpl(topologyID, execToSlotAfterRebalance, slotToResources, null);
        cluster.assign(newAssignment, true);
        ongoingMigrationProcess = false;
        //System.out.println(".....Migration..is done........" );


    }






    public static ExecuterMetricsDetails selectComponentForReplication(   List <ExecuterMetricsDetails> listexecuterTotalMetricsDetails, Cluster cluster, Topologies topologies)
    {
        List <ExecuterMetricsDetails>  selectedComponentsBottlnecks = new  ArrayList<ExecuterMetricsDetails>();
        Collections.sort(listexecuterTotalMetricsDetails,ExecuterMetricsDetails.CPUUsageComparator);
       // System.out.println(".........selectComponentForMigration......  "  );
       // System.out.println(".........selectComponentForMigration......listexecuterTotalMetricsDetails size  " +  listexecuterTotalMetricsDetails.size() );

        for (ExecuterMetricsDetails lstexecutersinfos : listexecuterTotalMetricsDetails) {
            if (lstexecutersinfos.getSuperviorID().equals(sourceNodeMigration) ) {
                double capacity = lstexecutersinfos.getCapacity();
               // System.out.println(".........selectComponentForMigration......  "  + lstexecutersinfos.getComponent() );
                /// for test
                //componetcapacityThereshold = 90.0;
                componetcapacityThereshold = 0.00;


               //   if (capacity > componetcapacityThereshold  )    /// original for real condition
                // if (capacity >= componetcapacityThereshold  && lstexecutersinfos.getComponent().contains("count")  )   // for Test
                if (capacity >= componetcapacityThereshold  && lstexecutersinfos.getComponent().contains("splitter")  )   // for Test
                //if (capacity >= componetcapacityThereshold  && lstexecutersinfos.getComponent().contains("log")  )   // for Test
                {

                    //System.out.println(".........selectComponent  ForReplication...is selected...  "   );
                    selectedComponentsBottlnecks.add(lstexecutersinfos);
                }

            }
        }


        ExecuterMetricsDetails finalTargetComponent=null;
        if (selectedComponentsBottlnecks.size() > 0) {
            finalTargetComponent = selectedComponentsBottlnecks.get(0);

        }

        return finalTargetComponent;
    }






    public static List <ExecutorDetails> selectComponentForMigration(   List <ExecuterMetricsDetails> listexecuterTotalMetricsDetails, ExecuterMetricsDetails finalTargetComponentReplication   , Cluster cluster, Topologies topologies)
    {
        List <ExecutorDetails>  selectedExecuters = new  ArrayList<ExecutorDetails>();
        Collections.sort(listexecuterTotalMetricsDetails,ExecuterMetricsDetails.CPUUsageComparator);

            //// we retrieve the last item of selected bin which is an executer
        ExecutorDetails lastExecuterSelected =null;
        for (Entry  <String ,LinkedList< ExecutorDetails> >  supervisorexecuter : supervisorsExecutersLinkedList.entrySet())
        {
            String  idSupervisor = supervisorexecuter.getKey();
            LinkedList<ExecutorDetails> linkedlistExecuters  = supervisorexecuter.getValue();
            if (idSupervisor.equals( sourceNodeMigration))
            {
                lastExecuterSelected=linkedlistExecuters.getLast();
                break;
            }
        }

        System.out.println(".........select Executed..Bin.........  "  + lastExecuterSelected );

        /// we try to retrieve the name of component of the previous executer
        String compSelected ="";
        List<ExecutorDetails> listExecuters =null;

        for (Entry  <String, List<ExecutorDetails>>  comptoexecuter : compToExecuters.entrySet()) {
            String  component = comptoexecuter.getKey();
            listExecuters  = comptoexecuter.getValue();
            for (ExecutorDetails  lstExecuters : listExecuters) {

                if (lstExecuters.toString().equals(  lastExecuterSelected.toString()   ))
                {
                    compSelected  = component;
                    selectedExecuters = listExecuters;

                     adjSourceworker = lstExecuters.getStartTask();
                     adjtargetworker = lstExecuters.getEndTask();

                    break;
                }

            }
        }



       Double cpuUsageComponentReplication = finalTargetComponentReplication.getComponentCPUUsage();

      // System.out.println(".........select component.for replication cpuUsageComponentReplication.........  "  + cpuUsageComponentReplication );


       // System.out.println(".........select component.for replication listexecuterTotalMetricsDetails....size.....  "  + listexecuterTotalMetricsDetails.size() );

        for (ExecuterMetricsDetails lstexecutersinfos : listexecuterTotalMetricsDetails) {

           // System.out.println(".........select component.for replication lstexecutersinfos.........  "  + lstexecutersinfos.getComponent() );

            if (lstexecutersinfos.getComponent().equals(compSelected) ) {
                double cpuUsageComponentMigration = lstexecutersinfos.getComponentCPUUsage() ;

                int numberExecuters = listExecuters.size() ;

                cpuUsageComponentMigration = cpuUsageComponentMigration * numberExecuters ;

               // System.out.println(".........select component.for replication cpuUsageComponentMigration.........  "  + cpuUsageComponentMigration );

                if (   cpuUsageComponentMigration < ( cpuUsageComponentReplication /2)  )   // for Test
                {
                    //System.out.println(".........selectComponent  ForReplication...Not greater equal...  "   );
                    selectedExecuters = null;

                }

            }
        }

        return selectedExecuters;
    }






    public static void elasticityReturnComponent(  List <ComponentCPUMemoryInfo> listcomponentCPUMemoryInfo, Cluster cluster, Topologies topologies) {


      //we check the input rate if it is less than landa then we do reverse migration

       if (rebalaceIsDone && testcounter >= 5  )
      // if (rebalaceIsDone && testcounter >= 5 && inputRateTopology < landa )    /// In real Test if the landa decreases
            {

                ongoingMigrationProcess = true;
                if (testcounter == 5) {
                    MigrationScheduler.rebalanceComponentElasticity(topologyName, componentNameMigrated);
                    previousTupleEmitted = 0L;
                    return;

                }

                if (testcounter == 6) {

                    updateAssignmentReverseMigration(cluster);
                    previousTupleEmitted = 0L;
                    System.out.println("...........elasticityComponentInfosList........back to its original worker node..........");
                    previousTupleEmitted = 0L;
                    rebalaceIsDone = false;

                    ongoingMigrationProcess = false;
                    //turnToMoveToOtherWorkerNode++;

                    isMigrating = false;
                    FirstRun = false;
                    inputRateTopology = 0L;
                    previousTupleEmitted = 0L;
                    testcounter = 0;
                    testcounterMigration = 0;
                    reverseMigrating = true;
                    //TheresholdInputRateMigration = 30000L;
                    TheresholdInputRateMigration = 40000L;

                }


       }

        //boolean a= true; if(a) return;
        Double targegtWorkerNoneCpuLoad= supervisor2workerloadInfo(targetNodeMigration);
        //System.out.println(".............elasticityComponentInfosList.size......targegtWorkerNoneCpuLoad........." +  targegtWorkerNoneCpuLoad  );
        Double targegtWorkerNoneCpuLoadElasticity=0.0;
        if ( elasticityComponentInfosList.size() > 0 && targegtWorkerNoneCpuLoad >= cpuUsageSecondThereshold && testcounter >= 3  ) {
            targegtWorkerNoneCpuLoadElasticity =  targegtWorkerNoneCpuLoad;
        }



        // if (    elasticityComponentInfosList.size() > 0  && testcounter >= 7 )   /// for manual test
        if (    elasticityComponentInfosList.size() > 0  )   /// for manual test
        {

           // System.out.println("   ....Elasticity....Check...1.." );

            int cnt =0;
            List<Integer> reverseMigrationfinished = new ArrayList<Integer>();
            // displayListElasticityComponent();
            for (ElasticityComponentInfo lstmigration : elasticityComponentInfosList) {   /// if any migration has ocuured
                Map<String, Map<String, Map<Double, Double>>> topologyCompponentsInfo = new HashMap<>();
                topologyCompponentsInfo = GetExecuterDetails.getExecuterDetailsDaemon();
                //System.out.println(":::::::::selctComponentForMigration::::::topologyCompponentsInfo::::" + topologyCompponentsInfo);
                List<ExecuterMetricsDetails> listexecuterTotalMetricsDetails = new ArrayList<ExecuterMetricsDetails>();
                List<ExecuterMetricsDetails> listexecuterCandidateMigrationMetricsDetails = new ArrayList<ExecuterMetricsDetails>();
                listexecuterTotalMetricsDetails = createTotalListComponentsInfo(topologyCompponentsInfo,listcomponentCPUMemoryInfo,cluster);

                //displayListComponent( listexecuterTotalMetricsDetails ) ;
                String sourcetopologyID = lstmigration.getTopologyID();
                String[] arrSplit = sourcetopologyID.split("-");
                String  sourcetopologyName =arrSplit[0];
                String sourceComponent = lstmigration.getComponent();
                String targetNodeMigration = lstmigration.getTargetSuperviorID();
                WorkerSlot targetWorkerslot = lstmigration.getSourceWorkerSlot();  /// we select the previous source for the target in order to reverse the process
                WorkerSlot sourceWorkerSlot = lstmigration.getTargetWorkerSlot();

                String targetTopologyID =lstmigration .getTargetTopologyID();
                //System.out.println("   .....elasticity...Before..target .Topology....."+  targetTopologyID );

                List<ExecutorDetails> executorToAssignOnTotal = lstmigration.getExecutorToAssignOnTotal();
                for (ExecuterMetricsDetails lstexecutersinfos : listexecuterTotalMetricsDetails) {
                    //if ( lstexecutersinfos.getTopologyID().equals(sourcetopologyID) &&  lstexecutersinfos.getComponent().equals(sourceComponent) && lstexecutersinfos.getWorkerSlot() .equals(lstmigration.getSourceWorkerSlot().toString() ) ) {
                       // System.out.println("   ....Elasticity....Check...2.." );

                        //  double capacity = lstexecutersinfos.getCapacity();
                        double CPUUsage = lstexecutersinfos.getComponentCPUUsage();
                        //  System.out.println("   ....component....CPUUsage....."+  CPUUsage );
                        //componetcapacityThereshold = 90.0;


                        //if (CPUUsage <  componetCpuUsageTheresholdReturn)    /// when capacity of operator is less than thereshold we return the operator to its original workernode
                        if (reverseMigrating)

                        {
                            System.out.println("   ....Elasticity....activate...3.." );
                            reverseMigrating =false;
                            // MigrationScheduler.commitReturnMigration(topologies, cluster, sourcetopologyID, sourceComponent, executorToAssignOnTotal,targetWorkerslot,sourceWorkerSlot);
                            Double  sourceTopologyCompleteLatencyAfter=  getTopologyCompleteTime(sourcetopologyID);
                            Double  targetTopologyCompleteLatencyAfter=  0.0;
                            if (!targetTopologyID.equals("") ) {
                                targetTopologyCompleteLatencyAfter=  getTopologyCompleteTime(targetTopologyID);
                            }

                            lstmigration.setTargetTopologyCompleteLatencyAfter( targetTopologyCompleteLatencyAfter ) ;
                            lstmigration.setSourceTopologyCompleteLatencyAfter( sourceTopologyCompleteLatencyAfter ) ;

                            Double sourceTopologyCompleteLatencyBefore= lstmigration.getSourceTopologyCompleteLatencyBefore();
                            Double targetTopologyCompleteLatencyBefore= lstmigration.getTargetTopologyCompleteLatencyBefore() ;



                           // MigrationScheduler .rebalanceComponentElasticity( sourcetopologyName ) ;
                            long end = System.currentTimeMillis();
                            lstmigration.setEndTime(end);

                            long start = lstmigration.getStartTime();
                            end = lstmigration.getEndTime();
                            float migrationduration =1;
                            migrationduration = (end - start) / 1000F;

                            Map<Double, Integer>  migrationCost =calculateCost( sourceTopologyCompleteLatencyBefore,  targetTopologyCompleteLatencyBefore,  sourceTopologyCompleteLatencyAfter , targetTopologyCompleteLatencyAfter,migrationduration , targegtWorkerNoneCpuLoadElasticity );

                            DataBaseManager.connectSql() ;
                            DataBaseManager.insertTable(lstmigration , migrationCost);
                            MigrationNumber++;

                            System.out.println("...........Elasticity............inserted to DB................MigrationNumber........."+MigrationNumber );
                            //testcounter =0;
                            //isMigrating = false;   /// in real test we can set this flag to true to allow other component to migrate in this
                           // FirstRun = true;
                            reverseMigrationfinished.add(cnt);
                       // } else {
                            /// we mesure the topology complete time in source and target
                      //  }
                    }
                }
                cnt ++;
            }

            /// remove items that their reverse migration has been completed
            for (int indexcompleted : reverseMigrationfinished) {
                elasticityComponentInfosList.remove(indexcompleted);
            }

        }



    }





    public static void selectFinalTargetNode( Cluster cluster, Topologies topologies ,  Map<String, Double> sortedCompare  ,List <ComponentCPUMemoryInfo> listcomponentCPUMemoryInfo  )
    {

        System.out.println("....................selectFinalTargetNode...............44.........." );

        Map<String, String> selectedComponent =  new HashMap<>();
        Map <String , Map <String , Map<Double, Double>>>  topologyCompponentsInfo  =  new HashMap<>();
        topologyCompponentsInfo =GetExecuterDetails.getExecuterDetailsDaemon();
        List <ExecuterMetricsDetails>  listexecuterTotalMetricsDetails = new  ArrayList<ExecuterMetricsDetails>();
        listexecuterTotalMetricsDetails = createTotalListComponentsInfo(topologyCompponentsInfo,listcomponentCPUMemoryInfo, cluster );


        for ( Entry<String, Double> entry : sortedCompare.entrySet()) {

            String node = entry.getKey();
            Double distance = entry.getValue();

            //Double sourceNodeMigration = sourceMigration;

            System.out.println(".........node......"+  node );
            System.out.println(".........distance......"+  distance );
//

        }





    }


    public static  void getComponentPageInfo( String  topologyID ,Cluster cluster ) {


        String component = "";
        Map<String, Component> topologyComp = null;
        //topologexecuters.clear() ;
        //Set<ExecutorDetails> topologexecuters = null;
        // Collection<TopologyDetails>  topologyDetail = topologies.getTopologies();

        for (TopologyDetails topology : cluster.getTopologies()) {

            String topologyId = topology.getId().toString();
            if (topologyId.toString().equals( topologyID )) {
                double cpuNeeded = topology.getTotalRequestedCpu();
                topologyComp = topology.getComponents();
                topologexecuters = topology.getExecutors();
                break;
            }else
            {
                continue;
            }
           // System.out.println(".......topologexecuters...." + topologexecuters);

        }

        String strComp = "";
        Component comp = null;

        for (Entry<String, Component> entry : topologyComp.entrySet()) {

            strComp = entry.getKey();
            comp = entry.getValue();
            List<ExecutorDetails> lstExecs=  comp.getExecs();
           // System.out.println(".......str....." + strComp + ".......comp....." + comp);

        Map<String, Object> resultGetComponentPage = new HashMap<>();
        resultGetComponentPage = GetExecuterDetails.getComponentPage(topologyID, strComp);
         //   resultGetComponentPage = GetExecuterDetails.getComponentPage(topologyID, "counter");

       //  System.out.println(".....Bin Packing.  getComponentPageInfo..  "+ resultGetComponentPage);

        Double totalCpu = 0.0;
        Double totalMem = 0.0;
        for (Entry<String, Object> entry1 : resultGetComponentPage.entrySet()) {
            String str1 = "";
            Object obj = null;
            str1 = entry1.getKey();
            obj = entry1.getValue();
             // System.out.println(".........str....  " +str1  +"......obj......." + obj);
            if (str1.equals("requestedCpu"))  totalCpu = Double.parseDouble(obj.toString());
            if (str1.equals("requestedMemOnHeap")) totalMem = Double.parseDouble(obj.toString());
        }

            int numberofExecutersPerComponent=lstExecs.size();

            for (ExecutorDetails lstexecsAssignResources : lstExecs) {
               // System.out.println(".....Executors....lstexecsAssignResources..  " + lstexecsAssignResources);
                Map<Double, Double> capacityExecuters = new HashMap<>();
                capacityExecuters.put(totalCpu/numberofExecutersPerComponent, totalMem/numberofExecutersPerComponent);
                executersResources.put (lstexecsAssignResources,capacityExecuters );

            }

          //  System.out.println("..getComponentPageInfo      .executersResources..  " +   executersResources );

        Map<Double, Double> capacity = new HashMap<>();
        capacity.put(totalCpu, totalMem);
        componentsResources.put (strComp,capacity );











    }



    }


    public static Long  getTopologyInputRate( String  topologyID ) {

        Long topologyInputRate;
        //System.out.println(".....getTopologyInputRate........getTopologyInputRate........." + topologyID);
        //  Map<String, Map<String, Double>> topologyycompleteTime =  MonitorScheduling.getTopologyExecuteCompleteTime( topologyID);

        if (topologyID.equals("") ) {
            return 0L;
        }else {
             topologyInputRate = GetExecuterDetails.getTopologyInputRate(topologyID);
        }
        // System.out.println(".....topologyycompleteTime::::::::::"+ topologyID + ":::" + topologyycompleteTime);
        return topologyInputRate;

    }





    public static Double  getTopologyCompleteTime( String  topologyID ) {

        // System.out.println(".....getTopologyCompleteTime::::::topologyID::::" + topologyID);
        //  Map<String, Map<String, Double>> topologyycompleteTime =  MonitorScheduling.getTopologyExecuteCompleteTime( topologyID);

        Double topologyycompleteTime =  GetExecuterDetails.getTopologyExecuteCompleteTime( topologyID );
        // System.out.println(".....topologyycompleteTime::::::::::"+ topologyID + ":::" + topologyycompleteTime);
        return topologyycompleteTime;

    }

    public static void  getTopologyInfoLoggerRstorm( Cluster cluster, Topologies topologies )
    {

        // return topologyycompleteTime;



        System.out.println(".....getTopologyInfoLoggerRstorm........." );


        String topologyId="";
        for (TopologyDetails  topology : cluster.getTopologies()) {



            topologyId = topology.getId().toString();
            //System.out.println(".........getTopologyInfoLoggerRstorm......topologyId  "  + topologyId );

        }


        if (!  topologyId.equals("") ) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Calendar cal = Calendar.getInstance();
            dateFormat.format(cal.getTime());

            double executelatecncy = 0.0;

            String strexecutelatecncy = Double.toString(executelatecncy);
            Double topologyycompleteTime = GetExecuterDetails.getTopologyExecuteCompleteTime(topologyId);
            String strtopologyycompleteTime = Double.toString(topologyycompleteTime);

            Long topologyFailedNumber = GetExecuterDetails.getTopologyFailedNumber(topologyId);

            // String  strtopologyFailedNumber= Double.toString(topologyFailedNumber);
            //componentExecuteLatency = 0;
            String strInfoView = dateFormat.format(cal.getTime()) + " ..TopologyCompleteTime.. " + topologyycompleteTime + " ..Failed.. " + topologyFailedNumber + " ..ComponentExecuteLatency.. " + strtopologyycompleteTime;
            String strInfoLogger = topologyycompleteTime + "," + topologyFailedNumber + "," + strtopologyycompleteTime;

            //System.out.println(strInfoView);
            logger(strInfoLogger,"Rstorm",true);
        }



    }


    public static void  getTopologyInfoLogger( Cluster cluster, Topologies topologies )
   // public static void  getTopologyInfoLogger( Cluster cluster, Topologies topologies ,List <ComponentCPUMemoryInfo> listcomponentCPUMemoryInfo)
    {
        Double result =0.0;
        String topologyID="";
        double componentExecuteLatency=0.0;
        Map<String, String> selectedComponent =  new HashMap<>();
        Map <String , Map <String , Map<Double, Double>>>  topologyCompponentsInfo  =  new HashMap<>();
        topologyCompponentsInfo =GetExecuterDetails.getExecuterDetailsDaemon();
        List <ExecuterMetricsDetails>  listexecuterTotalMetricsDetails = new  ArrayList<ExecuterMetricsDetails>();

        //listexecuterTotalMetricsDetails = createTotalListComponentsInfo(topologyCompponentsInfo,listcomponentCPUMemoryInfo, cluster );
        listexecuterTotalMetricsDetails = createTotalListComponentsInfoRstorm(topologyCompponentsInfo, cluster );

        // System.out.println(".........getTopologyCompleteTime......lstexecutersinfos size  "  + listexecuterTotalMetricsDetails.size() );
        for (ExecuterMetricsDetails lstexecutersinfos : listexecuterTotalMetricsDetails) {
            // if (lstexecutersinfos.getSuperviorID().equals(sourceNodeMigration) ) {

            topologyID = lstexecutersinfos.getTopologyID() ;

            //if (lstexecutersinfos.getComponent().equals("counter"))
            if (lstexecutersinfos.getComponent().equals("splitter"))
            {
                componentExecuteLatency=  lstexecutersinfos.getexecuteLatency();
            }


            //  if (lstexecutersinfos.getComponent().contains("count"))
            if (lstexecutersinfos.getComponent().contains("splitter"))
            {
                componentExecuteLatency=  lstexecutersinfos.getexecuteLatency();
            }


        }


        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        dateFormat.format(cal.getTime());

        double executelatecncy = componentExecuteLatency;
        if (listexecuterTotalMetricsDetails.size() >0) {


            //displayListComponent( listexecuterTotalMetricsDetails);
            String  strexecutelatecncy= Double.toString(executelatecncy);
            Double topologyycompleteTime = GetExecuterDetails.getTopologyExecuteCompleteTime(topologyID);
            String  strtopologyycompleteTime= Double.toString(topologyycompleteTime);

            Long topologyFailedNumber = GetExecuterDetails.getTopologyFailedNumber(topologyID);

            // String  strtopologyFailedNumber= Double.toString(topologyFailedNumber);
            //componentExecuteLatency = 0;
            String strInfoView = dateFormat.format(cal.getTime()) + " ..TopologyCompleteTime.. " + topologyycompleteTime + " ..Failed.. " + topologyFailedNumber + " ..ComponentExecuteLatency.. " + strexecutelatecncy;
            String strInfoLogger = topologyycompleteTime + "," + topologyFailedNumber + "," + strexecutelatecncy;

            System.out.println(strInfoView);
            logger(strInfoLogger,"topologyInfo",true);

        }

        // return topologyycompleteTime;


    }








    public static  List <ExecuterMetricsDetails> createTotalListComponentsInfoRstorm( Map <String , Map <String , Map<Double, Double>>> topologyCompponentsInfo, Cluster cluster )
    {
        /// this function creates total list from metrics information of components which can be used for selecting from them
        String topologyID ="";
        int port =0;
        String host = "";
        String SupervisorId ="";
        String WorkerSlot ="";
        String executers ="";
        Double capacity=0.0;
        Double executelatency=0.0;
        List <ExecuterMetricsDetails>  listexecuterTotalMetricsDetails = new  ArrayList<ExecuterMetricsDetails>();
        int cnt=0;
        for (Entry  <String , Map <String , Map<Double, Double>>> entry : topologyCompponentsInfo.entrySet()) {
            cnt++;
            topologyID = entry.getKey();
            Map <String , Map<Double, Double>>  componenetsInfo = entry.getValue();
            for (Entry  <String , Map<Double, Double>>  entry2 : componenetsInfo.entrySet()) {

                String componet = entry2.getKey();  /// we should pars this string to extract port & host
                String[] arrSplit = componet.split("::");
                componet = arrSplit[0];
                host = arrSplit[1];
                port = Integer.parseInt(arrSplit[2]);
                executers = arrSplit[3];
                List<SupervisorDetails> lsthostToSupervisor = cluster.getSupervisorsByHost(host);
                for (SupervisorDetails hostToSupervisor : lsthostToSupervisor)
                {
                    SupervisorId= hostToSupervisor.getId();
                }
                Map<Double, Double> componentCapacityExecute =entry2.getValue();
                for (Entry  <Double, Double>  entry3 : componentCapacityExecute.entrySet()) {
                    ExecuterMetricsDetails executerMetricsDetails =new ExecuterMetricsDetails();
                    if (cnt == 1 ) {
                        capacity = 72.0;
                    }else
                    {
                        capacity = entry3.getKey();
                    }
                    executelatency = entry3.getValue();

                    executerMetricsDetails.setTopologyID(topologyID);
                    executerMetricsDetails.setSuperviorID(SupervisorId);
                    executerMetricsDetails.setComponent(componet);
                    executerMetricsDetails.setHost(host);
                    executerMetricsDetails.setPort(port);
                    executerMetricsDetails.setWorkerSlot(SupervisorId+":"+port);
                    executerMetricsDetails.setCapacity(capacity);
                    executerMetricsDetails.setExecuteLatency(executelatency);
                    //executerMetricsDetails.setExecuters(executers);
                    executerMetricsDetails.setExecuters("3-7");

                    /// we compare the componentCPUMemoryInfo with the previously gatherde information of components to complete executerMetricsDetails class
//                    for (ComponentCPUMemoryInfo componentCPUMemoryInfo : listcomponentCPUMemoryInfo)
//                    {
//                        if (SupervisorId.equals(componentCPUMemoryInfo.supervisorId))
//                        {
//                            if ( componet.contains (componentCPUMemoryInfo.name))
//                            {
//                                executerMetricsDetails.setComponentCPUUsage(componentCPUMemoryInfo.CPUUsage );
//                                executerMetricsDetails.setComponentMemoryUsage(componentCPUMemoryInfo.memoryUsage ) ;
//                                // System.out.println("  .......createTotalListComponentsInfo....CPUUsage....  " + componet + "..CPU.."+ componentCPUMemoryInfo.CPUUsage + "..Memory.."+ componentCPUMemoryInfo.memoryUsage  );
//                            }
//                        }
//                    }
                    /////


                    listexecuterTotalMetricsDetails.add(executerMetricsDetails);
                }
            }
        }
        // System.out.println(":::::::::::::listexecuterTotalMetricsDetails:::::::::lst size::::::" +listexecuterTotalMetricsDetails.size() );

        return listexecuterTotalMetricsDetails;
    }









    public static  List <ExecuterMetricsDetails> createTotalListComponentsInfo( Map <String , Map <String , Map<Double, Double>>> topologyCompponentsInfo,List <ComponentCPUMemoryInfo> listcomponentCPUMemoryInfo, Cluster cluster )
    {
        /// this function creates total list from metrics information of components which can be used for selecting from them
        String topologyID ="";
        int port =0;
        String host = "";
        String SupervisorId ="";
        String WorkerSlot ="";
        String executers ="";
        Double capacity=0.0;
        Double executelatency=0.0;
        List <ExecuterMetricsDetails>  listexecuterTotalMetricsDetails = new  ArrayList<ExecuterMetricsDetails>();
        int cnt=0;
        for (Entry  <String , Map <String , Map<Double, Double>>> entry : topologyCompponentsInfo.entrySet()) {
            cnt++;
            topologyID = entry.getKey();
            Map <String , Map<Double, Double>>  componenetsInfo = entry.getValue();
            for (Entry  <String , Map<Double, Double>>  entry2 : componenetsInfo.entrySet()) {

                String componet = entry2.getKey();  /// we should pars this string to extract port & host
                String[] arrSplit = componet.split("::");
                componet = arrSplit[0];
                host = arrSplit[1];
                port = Integer.parseInt(arrSplit[2]);
                executers = arrSplit[3];
                List<SupervisorDetails> lsthostToSupervisor = cluster.getSupervisorsByHost(host);
                for (SupervisorDetails hostToSupervisor : lsthostToSupervisor)
                {
                    SupervisorId= hostToSupervisor.getId();
                }
                Map<Double, Double> componentCapacityExecute =entry2.getValue();
                for (Entry  <Double, Double>  entry3 : componentCapacityExecute.entrySet()) {
                    ExecuterMetricsDetails executerMetricsDetails =new ExecuterMetricsDetails();
                    if (cnt == 1 ) {
                        capacity = 72.0;
                    }else
                    {
                        capacity = entry3.getKey();
                    }
                    executelatency = entry3.getValue();

                    executerMetricsDetails.setTopologyID(topologyID);
                    executerMetricsDetails.setSuperviorID(SupervisorId);
                    executerMetricsDetails.setComponent(componet);
                    executerMetricsDetails.setHost(host);
                    executerMetricsDetails.setPort(port);
                    executerMetricsDetails.setWorkerSlot(SupervisorId+":"+port);
                    executerMetricsDetails.setCapacity(capacity);
                    executerMetricsDetails.setExecuteLatency(executelatency);
                   // executerMetricsDetails.setExecuters(executers);
                    executerMetricsDetails.setExecuters("3-7");

                    /// we compare the componentCPUMemoryInfo with the previously gatherde information of components to complete executerMetricsDetails class
                    for (ComponentCPUMemoryInfo componentCPUMemoryInfo : listcomponentCPUMemoryInfo)
                    {
                        if (SupervisorId.equals(componentCPUMemoryInfo.supervisorId))
                        {
                            if ( componet.contains (componentCPUMemoryInfo.name))
                            {
                                executerMetricsDetails.setComponentCPUUsage(componentCPUMemoryInfo.CPUUsage );
                                executerMetricsDetails.setComponentMemoryUsage(componentCPUMemoryInfo.memoryUsage ) ;
                               //  System.out.println("  .......createTotalListComponentsInfo....CPUUsage....  " + componet + "..CPU.."+ componentCPUMemoryInfo.CPUUsage + "..Memory.."+ componentCPUMemoryInfo.memoryUsage  );
                            }
                        }
                    }
                    /////


                    listexecuterTotalMetricsDetails.add(executerMetricsDetails);
                }
            }
        }
        // System.out.println(":::::::::::::listexecuterTotalMetricsDetails:::::::::lst size::::::" +listexecuterTotalMetricsDetails.size() );

        return listexecuterTotalMetricsDetails;
    }


    public static void supervisorQvalueInitilize( String fileName)

    {


        System.out.println("...................supervisorQvalueInitilize...........................  "  );
        String fileLoggerName = "/home//work/apache-storm-2.1.0/logs/" + fileName + ".log";




        try {


            File file=new File(fileLoggerName);    //creates a new file instance
            FileReader fr=new FileReader(file);   //reads the file
            BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
            StringBuffer sb=new StringBuffer();    //constructs a string buffer with no characters
            String line;
            while((line=br.readLine())!=null)
            {
                sb.append(line);      //appends line to string buffer
                sb.append("\n");     //line feed
            }
            fr.close();    //closes the stream and release the resources

           // System.out.println("...................supervisorQvalueInitilize...........................  "  + sb );

            String[] stringTotal = sb.toString().split(",");
            //System.out.println("...................supervisorQvalueInitilize...............stringTotal.length............  "  + stringTotal.length );
            if ( stringTotal.length > 0 )
            {

                for (int i=0;i<stringTotal.length; i++)
                {

                   // stringCPUMemory =

                    //System.out.println("...................supervisorQvalueInitilize...............stringTotal............  "  + stringTotal[i] );
                    String[]  supervisorQvalue = stringTotal[i].split("=");
                    String supervisorId = supervisorQvalue[0];
                    String qvaluestr = supervisorQvalue[1];
                    Double  qvalue=Double.parseDouble(qvaluestr);
                    supervisorsQValue.put( supervisorId, qvalue );
                }

            }

            System.out.println("...................supervisorQvalueInitilize...............supervisorsQValue..From File..........  "  + supervisorsQValue );



        } catch (IOException e) {
            e.printStackTrace();
        }


    }


        public static void supervisorQvalueLogger( )
    {

        String info ="";
        for (Entry<String,Double> entry : supervisorsQValue.entrySet()) {

            String SupervisorId = entry.getKey();
            Double qValue = entry.getValue();




           //info =info +epochNumber+ "," +SupervisorId +"="+ qValue.toString()+",";
            info =info + "," +SupervisorId +"="+ qValue.toString()+",";

           // info =info +SupervisorId +"="+ qValue.toString()+",";






        }


        info = epochNumber+ ","+info ;
        System.out.println(info);
       // logger( info , "supervisorQvalue" ,false);
       // info =info
        logger( info , "supervisorQvalue" ,true);





    }



    public static void benchMarckCpuLoadLogger(String source, Map <String , Double > supervisorsBenchmarkExecute , List<WorkerNodeUsageDetails> workerNodeUsageDetailsList )
    {

        String result="";

        //map.get( "keyname" )).get( "nestedkeyname" );
        Double sourceExecuteLatency =supervisorsBenchmarkExecute.get(source);
        // String  SupervisorId =source;

        // System.out.println(SupervisorId+","+  cpuusage+"," + executelatency );




        // System.out.println("................selectTargetNode......sourceExecuteLatency..................." + sourceExecuteLatency );
        Map <String,Double> resultCompare = new HashMap<>();

        //Map <String, Map<String, Long>> queryResult = new HashMap<>();
        for (Entry<String,Double> entry : supervisorsBenchmarkExecute.entrySet()) {

            String  SupervisorId =entry.getKey();
            Double executelatency =entry.getValue();
            // System.out.println("................selectTargetNode......SupervisorId..................." + SupervisorId );

            if (!SupervisorId.equals(source) )
            {
                // Double difference =  sourceExecuteLatency - executelatency;
                // difference= Math.abs(difference);
                //System.out.println(":::::::::sourceExecuteLatency::::"+  difference +"::"+ sourceExecuteLatency + ":::"+executelatency);
                for (WorkerNodeUsageDetails lst : workerNodeUsageDetailsList) {   /// iterate through the list to check if weather the nodes are overutilized or not
                    String SupervisoridLst = lst.getSupervisorID();
                    Double cpuusage = lst.getCpuUsage();
                    if (SupervisorId.equals(SupervisoridLst))
                    {
                        System.out.println(SupervisorId+","+  cpuusage+"," + executelatency );
                        logger( SupervisorId+","+  cpuusage+"," + executelatency , "benchMarkTasks",true);
                        break;
                        // return;



                    }
                }
            }
            //memoryusage = lst.getMemoryUsage();
        }




    }


    public static LinkedHashMap<String, Double> setPriority( int adjSourceworker, int adjtargetworker, double cpuUsage , double cpuThreshold , LinkedHashMap<String, Double> sortedCompare)
    {

        LinkedHashMap<String, Double> output = new LinkedHashMap<>();

        for (Entry<String,Double> entry : supervisorsBenchmarkExecute.entrySet()) {

            String SupervisorId = entry.getKey();
            Double priority = (adjtargetworker / adjSourceworker) - (  cpuThreshold - cpuUsage );


            output.put(SupervisorId, priority);



        }


        return output;
    }


    public static LinkedHashMap<String, Double> selectTargetNode(String source, Map <String , Double > supervisorsBenchmarkExecute , List<WorkerNodeUsageDetails> workerNodeUsageDetailsList )
    {

        // System.out.println("....................selectTargetNode.............source............"+ source );

        String result="";

        //map.get( "keyname" )).get( "nestedkeyname" );
        Double sourceExecuteLatency =supervisorsBenchmarkExecute.get(source);
        // System.out.println("................selectTargetNode......sourceExecuteLatency..................." + sourceExecuteLatency );
        Map <String,Double> unsorted = new HashMap<>();
        //Map <String,Double> unsorted = new HashMap<>();

        //LinkedHashMap preserve the ordering of elements in which they are inserted
        LinkedHashMap<String, Double> resultCompare = new LinkedHashMap<>();

        //Map <String, Map<String, Long>> queryResult = new HashMap<>();
        for (Entry<String,Double> entry : supervisorsBenchmarkExecute.entrySet()) {

            String  SupervisorId =entry.getKey();
            Double executelatency =entry.getValue();
            // System.out.println("................selectTargetNode......SupervisorId..................." + SupervisorId );

            if (!SupervisorId.equals(source)   && ( sourceExecuteLatency >= executelatency )     )
            {
                Double difference =  sourceExecuteLatency - executelatency;

                difference= Math.abs(difference);
                 System.out.println(":::::::::sourceExecuteLatency::::"+  difference +"::"+ sourceExecuteLatency + ":::"+executelatency);
                for (WorkerNodeUsageDetails lst : workerNodeUsageDetailsList) {   /// iterate through the list to check if weather the nodes are overutilized or not
                    String SupervisoridLst = lst.getSupervisorID();
                    Double cpuusage = lst.getCpuUsage();
                    if (SupervisorId.equals(SupervisoridLst))
                    {
                        if (cpuusage < cpuUsageSecondThereshold) {
                        }


                       if  (difference >= 0) {
                           unsorted.put(SupervisorId, difference);
                       }
                    }
                }
            }
            //memoryusage = lst.getMemoryUsage();
        }

       // System.out.println("................selectTargetNode....before sorting..unsorted..................." + unsorted );



        unsorted.entrySet()
                .stream()
                .sorted(Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> resultCompare.put(x.getKey(), x.getValue()));

        return resultCompare;
    }

    public static Map<Double, Integer> calculateCost(Double sourceTopologyCompleteLatencyBefore, Double targetTopologyCompleteLatencyBefore, Double sourceTopologyCompleteLatencyAfter , Double targetTopologyCompleteLatencyAfter, float migrationduration , Double targegtWorkerNoneCpuLoadElasticity  )
    {


        Map<Double, Integer> tempvalues =  new HashMap<>();
        // int result =1;
        Double totalCost=0.0;
        Double cost=1.0;
        Double migrationTimeCost=1.0;
        int proper=1;




        System.out.println( ".....calculateCost::::sourceTopologyCompleteLatencyBefore "+sourceTopologyCompleteLatencyBefore);
        System.out.println( ".....calculateCost::::targetTopologyCompleteLatencyBefore "+targetTopologyCompleteLatencyBefore);
        System.out.println( ".....calculateCost::::sourceTopologyCompleteLatencyAfter "+sourceTopologyCompleteLatencyAfter);
        System.out.println( ".....calculateCost::::targetTopologyCompleteLatencyAfter "+targetTopologyCompleteLatencyAfter);
        System.out.println( ".....calculateCost::::targegtWorkerNoneCpuLoadElasticity "+ targegtWorkerNoneCpuLoadElasticity );


        if (targetTopologyCompleteLatencyBefore == 0) targetTopologyCompleteLatencyBefore=1.0;
        if (targetTopologyCompleteLatencyAfter == 0) targetTopologyCompleteLatencyAfter=1.0;

        Double MaxTopologyCompleteLatencySource =  sourceTopologyCompleteLatencyBefore + (sourceTopologyCompleteLatencyBefore * 0.20);
        Double MaxTopologyCompleteLatencyTarget =  targetTopologyCompleteLatencyBefore + (targetTopologyCompleteLatencyBefore * 0.20);

        System.out.println( ".....calculateCost::::MaxTopologyCompleteLatencySource "+MaxTopologyCompleteLatencySource);
        System.out.println( ".....calculateCost::::MaxTopologyCompleteLatencyTarget "+MaxTopologyCompleteLatencyTarget);



        Double  sourceTopologyVariations =  ( sourceTopologyCompleteLatencyAfter / MaxTopologyCompleteLatencySource );
        Double  targetTopologyVariations = ( targetTopologyCompleteLatencyAfter /MaxTopologyCompleteLatencyTarget ) ;

        System.out.println( ".....calculateCost::::sourceTopologyVariations "+sourceTopologyVariations);
        System.out.println( ".....calculateCost::::targetTopologyVariations "+targetTopologyVariations);

        sourceTopologyVariations = sourceTopologyVariations * 10;
        targetTopologyVariations = targetTopologyVariations * 10;

        System.out.println( ".....calculateCost::::sourceTopologyVariations "+sourceTopologyVariations);
        System.out.println( ".....calculateCost::::targetTopologyVariations "+targetTopologyVariations);
        System.out.println( ".....calculateCost::::migrationduration "+migrationduration);



        if (targegtWorkerNoneCpuLoadElasticity == 0) {
            migrationTimeCost = 0.0;
        }else
        {
            if  (migrationduration <120)
            {
                migrationTimeCost =1.0;
            }

        }



        System.out.println( ".....calculateCost::::migrationTimeCost "+  migrationTimeCost);

        // migrationTimeCost =10.0;
        double w1 = 0.33;
        double w2 = 0.33;
        double w3 = 0.33;

        totalCost = (w1 * sourceTopologyVariations ) +(w2 * targetTopologyVariations )+ (w3 * migrationTimeCost );
        System.out.println( ".....insertTable::::totalCost "+totalCost);

        cost =totalCost;   /// we are intended to minimize the cost. it means the higher cost has negative consequences

        if (cost < 15)
        {
            proper =1;
        }else
        {
            proper =0;
        }
        tempvalues.put(cost,proper);


        //result =proper;
        // tempvalues
        return tempvalues;
    }










    public static Double supervisor2workerloadInfo(String supervisorID)
    {
        //workerNodeUsageDetailsList.add(workerNodeUsageDetails);
        Double result=0.0;
        for (WorkerNodeUsageDetails lst : workerNodeUsageDetailsList) {   /// iterate through the list to check if weather the nodes are overutilized or not
            String Supervisorid = lst.getSupervisorID();
            Double cpuusage = lst.getCpuUsage();
            //System.out.println(":::::::::supervisor2workerloadInfo::::"+  Supervisorid + "::"+ cpuusage );
            if (supervisorID.equals(Supervisorid))
            {
                result = cpuusage;
            }
        }


        return result;

    }



    public static void displayListElasticityComponent ()
    {

        for (ElasticityComponentInfo elasticcomponent : elasticityComponentInfosList) {
            System.out.println(".........executermeticListElasticity...getsourceSuperviorID..." + elasticcomponent.getsourceSuperviorID());
            System.out.println(".........executermeticListElasticity...getTargetSuperviorID..." + elasticcomponent.getTargetSuperviorID());
            System.out.println(".........executermeticListElasticity...getSourceSuperviorName..." + elasticcomponent.getSourceSuperviorName());
            System.out.println(".........executermeticListElasticity...getTargetSuperviorName..." + elasticcomponent.getTargetSuperviorName());
            System.out.println(".........executermeticListElasticity....getSourceWorkerSlot.." + elasticcomponent.getSourceWorkerSlot());
            System.out.println(".........executermeticListElasticity....getTargetWorkerSlot.." + elasticcomponent.getTargetWorkerSlot());
            System.out.println(".........executermeticListElasticity....getSourceCapacity.." + elasticcomponent.getSourceCapacity());
            System.out.println(".........executermeticListElasticity....getSourceCPUUsage.." + elasticcomponent.getSourceCPUUsage());


            System.out.println(".........executermeticListElasticity...getTopologyID..." + elasticcomponent.getTopologyID());
            System.out.println(".........executermeticListElasticity...getTargetTopologyID..." + elasticcomponent.getTargetTopologyID());
            System.out.println(".........executermeticListElasticity....getSourceTopologyCompleteLatencyBefore.." + elasticcomponent.getSourceTopologyCompleteLatencyBefore() );
            System.out.println(".........executermeticListElasticity...getComponent..." + elasticcomponent.getComponent());
            System.out.println(".........executermeticListElasticity....getStartTime.." + elasticcomponent.getStartTime());
            System.out.println(".........executermeticListElasticity....getEndTime.." + elasticcomponent.getEndTime());
        }
    }


    public static void displayListComponent(List <ExecuterMetricsDetails> listexecuterTotalMetricsDetails )
    {


        for (ExecuterMetricsDetails executerMetricsDetails : listexecuterTotalMetricsDetails) {

            System.out.println(".........executermeticList...topology..."+ executerMetricsDetails.getTopologyID()  );
            System.out.println(".........executermeticList...supervisor..."+ executerMetricsDetails.getSuperviorID()  );
            System.out.println(".........executermeticList...component..."+ executerMetricsDetails.getComponent()  );
            System.out.println(".........executermeticList...host..."+ executerMetricsDetails.getHost()  );
            System.out.println(".........executermeticList...port..."+ executerMetricsDetails.getPort()  );
            System.out.println(".........executermeticList...capacity..."+ executerMetricsDetails.getCapacity()  );
            System.out.println(".........executermeticList....executers.."+ executerMetricsDetails.getExecuters()  );
            System.out.println(".........executermeticList....cpuusage.."+ executerMetricsDetails.getComponentCPUUsage()  );
            System.out.println(".........executermeticList....memeoryusage.."+ executerMetricsDetails.getComponentMemoryUsage()  );
            System.out.println(".........executermeticList....ExecuteLatency.."+ executerMetricsDetails.getexecuteLatency()  );
            System.out.println( "..............................................................................");
        }



    }

    public static void logger(String info, String fileName,Boolean append)
    {


        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        System.out.println(dateFormat.format(cal.getTime()));
        String fileLoggerName = fileName;


        // logger("..Online Scheduling Assignment " +  dateFormat.format(cal.getTime()));
        FileWriter fw = null;
        BufferedWriter bw = null;
        PrintWriter pw = null;
        try {
           // fw = new FileWriter("/home/ali/work/apache-storm-2.1.0/logs/OnlineScheduling.log", true);
           if (!append ) {
               fw = new FileWriter("/home/ali/work/apache-storm-2.1.0/logs/" + fileLoggerName + ".log", false);
           }else
           {
               fw = new FileWriter("/home/ali/work/apache-storm-2.1.0/logs/" + fileLoggerName + ".log", true);
           }

            bw = new BufferedWriter(fw);
            pw = new PrintWriter(bw);

            pw.println( dateFormat.format(cal.getTime())+"," + info+"/r/n");

            pw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static class ActionEvaluation
    {

        List<ExecutorDetails>   executerDetails =null;
        String  sourceSupervisorID =null;
        String  targetSupervisorID =null;
        Double costValueBeforMigration =0.0;
        Double costValueAfterMigration =0.0;
        Map<ExecutorDetails, WorkerSlot> execToSlotAfterRebalanceEvaluation =new HashMap<>();
        int epochnumber;
        Long inputRate = 0L;


    }


    public static class ComponentCPUMemoryInfo
    {

        String supervisorId ="";
        String name ="";
        Double CPUUsage =0.0;
        Double memoryUsage =0.0;


    }



    public static Double formateDoubleValue(Double value )
    {

       // DecimalFormat df2 = new DecimalFormat("#.######");
        DecimalFormat df2 = new DecimalFormat("#.####");

        String formateed = df2.format(value);
        Double formatedValue = Double.parseDouble(formateed);
        return formatedValue;

    }






}
