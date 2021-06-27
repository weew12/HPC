package org.apache.storm.scheduler;

import java.util.List;

public class BenchMarkMigration {



    public static void benchMarkMigrate(Cluster cluster, WorkerSlot sourceWorkerSlot, WorkerSlot targetWorkerSlot, List<ExecutorDetails> executorToAssignOnTargetWS, TopologyDetails topology ) {


        cluster.freeSlot(sourceWorkerSlot);
        System.out.println("::::benchMarkMigrate::::FirstTime:::::::::sourceWorkerSlot Freed::::::" + sourceWorkerSlot);
        cluster.assign(targetWorkerSlot, topology.getId(), executorToAssignOnTargetWS);
        System.out.println("::::benchMarkMigrate::::FirstTime:::::Transfered::to::targetWorkerSlot::::::" + targetWorkerSlot);


    }

}
