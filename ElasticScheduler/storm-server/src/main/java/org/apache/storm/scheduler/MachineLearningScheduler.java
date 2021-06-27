package org.apache.storm.scheduler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

public class MachineLearningScheduler {

    public static int totalNumberPropers =1;



    public static   String  BayesClassifyerNodes( Map<String, Double> sortedCompareWorkernodes , String sourceNodeMigration , String sourceComponentName,Double sourceComponentCPUUsage,String sourcetopologyName,Cluster cluster ) {

       String selectedNode="";


       return selectedNode;


    }

public static String workerNode2Supervisor( String workerNode , Cluster cluster){

    String result ="";
    Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();

    for (  Entry<String,  SupervisorDetails > entry : supervisordetls.entrySet()) {

        String id_supervisor = entry.getKey();
        SupervisorDetails supdetails = entry.getValue();
        String supervisorid = supdetails.getId() ;
        // System.out.println(".................workerNode2Supervisor............................." + supervisorid );
        ///test
        if   ( supervisorid.contains( workerNode ) ) {
            result = supervisorid;
        }


    }

    return result;



}



    public static   Double  BayesClassifyerInputRate(   Long  inputRateRange, String componentname) {

        //boolean migrate =false;
        Double migrationProbability=0.0;

        DataBaseManager.connectSql() ;


        double   totalProbalityGivenProper =  probabilityTotal();
        //System.out.println("......totalProbalityGivenProper::::" + totalProbalityGivenProper);

        double    componentprobability=  probabilityInputRateComponent(  inputRateRange ,componentname  );
        //System.out.println(".....componentprobability::::" + componentprobability);


        double Probality = totalProbalityGivenProper  *  componentprobability  ;
        // double Probality = totalProbalityGivenProper  *  componentprobability * targetWorkerNodeCpuLoadNormalDistribution  ;

        DecimalFormat df2 = new DecimalFormat("#.####");
        String formateed = df2.format(Probality);
        Double finalProbability = Double.parseDouble(formateed);
        migrationProbability = finalProbability;
        System.out.println("...Final.Probality....BayesClassifyerInputRate........" + finalProbability);






        return migrationProbability;
    }


        public static   ExecuterMetricsDetails  BayesClassifyer( List<ExecuterMetricsDetails> componentBottlenecksList, Cluster cluster, Double  targetWorkerNoneCpuLoad) {


        HashMap< ExecuterMetricsDetails,Double > componentProbability =new  HashMap<>();

        ExecuterMetricsDetails selectedComponent = new ExecuterMetricsDetails();
        DataBaseManager.connectSql() ;

        for (ExecuterMetricsDetails componentBottlenecks : componentBottlenecksList) {

            double capacity = componentBottlenecks.getCapacity();
            String component = componentBottlenecks.getComponent() ;
            String topologyID = componentBottlenecks.getTopologyID();
            //Double targetWorkerNodeCpuLoad = componentBottlenecks.get();
            String topologyName="";

            for (TopologyDetails  topology : cluster.getTopologies()) {
                String topologyId = topology.getId().toString();
                if (topologyId.equals(topologyID)) topologyName = topology.getName() ;
            }



            double   totalProbalityGivenProper =  probabilityTotal();
            //System.out.println("......totalProbalityGivenProper::::" + totalProbalityGivenProper);

            double    componentprobability=  probabilityComponent(  component,  topologyName);
           //System.out.println(".....componentprobability::::" + componentprobability);


           double    targetWorkerNodeCpuLoadNormalDistribution= probabilityNormalDistribution( "targetWorkerNodeCpuLoad" , targetWorkerNoneCpuLoad);
           //double    targetWorkerNodeCpuLoadNormalDistribution= probabilityNormalDistribution( "targetWorkerNodeCpuLoad" , 10.0);
           // System.out.println(".......targetWorkerNodeCpuLoadNormalDistribution::::" + targetWorkerNodeCpuLoadNormalDistribution);



            double    sourceCapacityNormalDistribution= probabilityNormalDistribution( "sourceCapacity" ,capacity);
            //System.out.println(".....sourceCapacityNormalDistribution::::" + sourceCapacityNormalDistribution);


            double Probality = totalProbalityGivenProper  *  componentprobability * targetWorkerNodeCpuLoadNormalDistribution * sourceCapacityNormalDistribution ;
           // double Probality = totalProbalityGivenProper  *  componentprobability * targetWorkerNodeCpuLoadNormalDistribution  ;

            DecimalFormat df2 = new DecimalFormat("#.####");
            String formateed = df2.format(Probality);
            Double finalProbability = Double.parseDouble(formateed);

           // System.out.println("...Final.Probality............" + finalProbability);


            componentProbability.put( componentBottlenecks ,Probality );  /// in the final we put in the map
        }



/// sorting

        HashMap<ExecuterMetricsDetails, Double> hm1 = sortByValue( componentProbability);

        for (Entry< ExecuterMetricsDetails,Double > entry : hm1.entrySet()) {

            ExecuterMetricsDetails executeinfo = entry.getKey();
            Double probability = entry.getValue();
            //System.out.println("....Machine Learning result: Map:: Sorted::" + executeinfo.getTopologyID() +"::" + executeinfo.getComponent()+"::"+probability);
            selectedComponent = executeinfo;
        }

       return selectedComponent;
    }









    public static float probabilityNormalDistribution( String field , Double input)
    {
        double value=1;
        int totalRecordsComponent = 1;
        int numberSuccess =1;

        int status =1;
        String  sqlQuery = "SELECT " + field +   " FROM MachineLearningModel WHERE proper =  "  + status +  ";";
       // System.out.println("::::::::probabilityNormalDistribution::sqlQuery::"+sqlQuery);
        ResultSet result = DataBaseManager.selectFromTables(sqlQuery);

        List<Double> lstFieldValue =new ArrayList<>();
        try {
            while ( result.next() ) {
                Double targetWorkerNodeCpuLoad = result.getDouble(field);
                lstFieldValue.add(targetWorkerNodeCpuLoad);


                // System.out.println(":::::targetWorkerNodeCpuLoad:::::"+targetWorkerNodeCpuLoad);
            }
            result.close();
            normalDistribution normalDis = new normalDistribution();
            value = normalDis.continuesProbability(lstFieldValue ,input );
           // System.out.println(":::::probabilityNormalDistribution::value:::"+value);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }






       // value =  (float)totalRecordsComponent / (float)totalNumberPropers ;



        //System.out.println("::::::::probabilityComponent::value::"+value);
        ;
        //value =  numberSuccess /totalRecords ;
        //System.out.println("::::::::probabilityTotal::value::"+value);
        return (float)value ;
    }



    public static float probabilityNode( String  targetSuperviorID)
    {

        float value=1;
        int totalRecordsComponent = 1;
        int numberSuccess =1;

        //System.out.println("::::::::probabilityComponent::value::");

        int status =1;
        String sqlQuery = "SELECT count(*) FROM MachineLearningModel WHERE  targetSuperviorID = '" + targetSuperviorID + "'" ;

        //System.out.println("::::::::probabilityComponent::sqlQuery::"+sqlQuery);
        ResultSet result = DataBaseManager.selectFromTables(sqlQuery);
        try {

            totalRecordsComponent = result.getInt(1);
            // System.out.println("::::::::probabilityComponent::totalRecords::"+totalRecordsComponent);
            result.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        //System.out.println("::::::::probabilityComponent::totalRecordsComponent::"+totalRecordsComponent);
        // System.out.println("::::::::probabilityComponent::totalNumberPropers::"+totalNumberPropers);
        value =  (float)totalRecordsComponent / (float)totalNumberPropers ;
        // System.out.println("::::::::probabilityComponent::value::"+value);
        ;
        //value =  numberSuccess /totalRecords ;
        //System.out.println("::::::::probabilityTotal::value::"+value);
        return (float)value ;



    }


    public static float probabilityInputRateComponent( Long inputRate, String componentId)
    {
        float value=1;
        int totalRecordsComponent = 1;
        int numberSuccess =1;

        //System.out.println("::::::::probabilityComponent::value::");

        int status =1;
        String sqlQuery = "SELECT count(*) FROM MachineLearningModel WHERE component = '" + componentId + "'" +     " and  proper = " + status   +     " and  inputRateTopology = " + inputRate +  ";";
        // String sqlQuery = "SELECT count(*) FROM MachineLearningModel WHERE component = '" + componentId + "'" +     " and  proper = " + status +  ";";


        //System.out.println("::::::::probabilityComponent::sqlQuery::"+sqlQuery);
        ResultSet result = DataBaseManager.selectFromTables(sqlQuery);
        try {

            totalRecordsComponent = result.getInt(1);
            // System.out.println("::::::::probabilityComponent::totalRecords::"+totalRecordsComponent);
            result.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        value =  (float)totalRecordsComponent / (float)totalNumberPropers ;

        //System.out.println("...........probabilityInputRateComponent........totalRecordsComponent.........." + totalRecordsComponent  );
        //System.out.println("...........probabilityInputRateComponent........totalNumberPropers.........." + totalNumberPropers  );


        //System.out.println("...........probabilityInputRateComponent........value.........." + value  );

        ;
        return (float)value ;
    }






    //public static float probabilityComponent( String componentId)
    public static float probabilityComponent( String componentId, String topologyname)
    {
        float value=1;
        int totalRecordsComponent = 1;
        int numberSuccess =1;

        //System.out.println("::::::::probabilityComponent::value::");

        int status =1;
        String sqlQuery = "SELECT count(*) FROM MachineLearningModel WHERE component = '" + componentId + "'" +   " and topologyID = '" + topologyname + "'" +  " and  proper = " + status +  ";";
       // String sqlQuery = "SELECT count(*) FROM MachineLearningModel WHERE component = '" + componentId + "'" +     " and  proper = " + status +  ";";

        //System.out.println("::::::::probabilityComponent::sqlQuery::"+sqlQuery);
        ResultSet result = DataBaseManager.selectFromTables(sqlQuery);
        try {

            totalRecordsComponent = result.getInt(1);
           // System.out.println("::::::::probabilityComponent::totalRecords::"+totalRecordsComponent);
            result.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        //System.out.println("::::::::probabilityComponent::totalRecordsComponent::"+totalRecordsComponent);
       // System.out.println("::::::::probabilityComponent::totalNumberPropers::"+totalNumberPropers);
        value =  (float)totalRecordsComponent / (float)totalNumberPropers ;
       // System.out.println("::::::::probabilityComponent::value::"+value);
        ;
        //value =  numberSuccess /totalRecords ;
        //System.out.println("::::::::probabilityTotal::value::"+value);
        return (float)value ;
    }





    public static float probabilityTotal()
    {
        float value=1;
        int totalRecords = 1;
        int numberSuccess =1;
        String sqlQuery ="SELECT count(*) from MachineLearningModel;";
       // System.out.println("::::::::probabilityTotal::totalRecords::"+sqlQuery);
        ResultSet result = DataBaseManager.selectFromTables(sqlQuery);
        try {

             totalRecords = result.getInt(1);
             //System.out.println("::::::::probabilityTotal::totalRecords::"+totalRecords);
             result.close();
            } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        int status =1;
        sqlQuery = "SELECT count(*) from MachineLearningModel where proper = " + status + ";";
         result = DataBaseManager.selectFromTables(sqlQuery);
        try {
            while (result.next()) {

                numberSuccess =result.getInt(1);

            }
            result.close();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        value =  (float)numberSuccess /(float)totalRecords ;
          totalNumberPropers = numberSuccess;
        return value;
    }









    public static   void    CalculateCost( ) {



    }






    public static HashMap<String, Double> sortByValueNodes(HashMap<String, Double> hm)
    {


        // Create a list from elements of HashMap
        List<Map.Entry<String, Double> > list = new LinkedList<Map.Entry<String, Double> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Double> >() {
            public int compare(Map.Entry<String, Double> o1,
                               Map.Entry<String, Double> o2)
            {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Double> temp = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;


    }



    public static HashMap<ExecuterMetricsDetails, Double> sortByValue(HashMap<ExecuterMetricsDetails, Double> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<ExecuterMetricsDetails, Double> > list =
                new LinkedList<Map.Entry<ExecuterMetricsDetails, Double> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<ExecuterMetricsDetails, Double> >() {
            public int compare(Map.Entry<ExecuterMetricsDetails, Double> o1,
                               Map.Entry<ExecuterMetricsDetails, Double> o2)
            {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<ExecuterMetricsDetails, Double> temp = new LinkedHashMap<ExecuterMetricsDetails, Double>();
        for (Map.Entry<ExecuterMetricsDetails, Double> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }



    public static class normalDistribution
    {

        // Function for calculating
        // variance
         double continuesProbability(List<Double> lst , Double input )
        {
            // Compute mean (average
            // of elements)
           // System.out.println("::::::::::::input::::::::::::::::input:::::" + input);

            Double[] dblArray = new Double[lst.size()];
            int  n=  lst.size();
            Double inputestimate =input ;
            dblArray = lst.toArray(dblArray);


            double sampleMean ;
            double sampleVariance ;
            double sampleStandardDeviation ;

            //Double probability =1.0;
            float  probability =1;

            double sum = 0;

            for (int i = 0; i < n; i++)
                sum += dblArray[i];
            double mean = (double)sum /
                    (double)n;
            sampleMean =mean;
           // System.out.println("::::::::::::mean::::::::::::::::totalmean:::::" + sampleMean);
            // Compute sum squared
            // differences with mean.
            double sqDiff = 0;
            for (int i = 0; i < n; i++)
                sqDiff += (dblArray[i] - mean) *
                        (dblArray[i] - mean);
            /// n-1   because we use sample standard deiation
            int m=n -1;
            sampleVariance = (double)sqDiff / m;
          //  System.out.println("::::::::::::sampleVariance:::::::::::::::::::::" + sampleVariance);
            sampleStandardDeviation = Math.sqrt(sampleVariance);
           // System.out.println("::::::::::::sampleStandardDeviation:::::::::::::::::::::" + sampleStandardDeviation);
            double part1 = 1 / (  Math.sqrt( (2 * 3.14) ) * sampleStandardDeviation);
            //System.out.println("::::::::::::part1:::::::::::::::::::::" + part1);
            double part2 = (( inputestimate - sampleMean) * ( inputestimate - sampleMean))   /    (2* sampleVariance)   ;
           // System.out.println("::::::::::::part2:::::::::::::::::::::" + part2);
            double part3 = Math.exp(part2);
           // System.out.println("::::::::::::part3:::::::::::::::::::::" + part3);
            probability = (float)part1 * (float)part3;
           // System.out.println("::::::::::::probability:::::::::::::::::::::" + probability);


           if (probability > 1) probability =1;

            return probability;
        }




    }





}
