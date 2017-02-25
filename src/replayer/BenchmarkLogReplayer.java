/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package replayer;

import agent.Metrics;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.log4j.Logger;
import protopeer.measurement.LogReplayer;
import protopeer.measurement.MeasurementLog;
import protopeer.measurement.MeasurementLoggerListener;

/**
 * Loads logs, calculates and prints measurement results.
 * Features:
 * - Computes per iteration and per time step
 * - Writes results to files for further processing: Each metric to its own file
 *   with one line per time step and a value for each iteration.
 * - Does a pretty overview of all the metrics, also saves it to allMetrics.txt
 * - Plays together with BenchmarkSimulationAgent
 * 
 * @author Evangelos
 */
public class BenchmarkLogReplayer {

    private static final Logger logger = Logger.getLogger(BenchmarkLogReplayer.class);
    
    private String expSeqNum;
    private String expID;
    private String resultID;

    private LogReplayer replayer;
    private final String coma=",";
    private boolean writeToFile;
    private String experimentToken = "experiment-";
    private String resultToken = "results";
    private String peerletsLogToken = "peerlets-log";

    private PrintWriter allMetricsOut;
    
    private PrintWriter linkLossOut;
    private PrintWriter linkFlowOut;
    private PrintWriter linkUtilizationOut;
    private PrintWriter epochPowerLossOut;
    private PrintWriter totalPowerLossOut;
    private PrintWriter totalTimeOut;
    private PrintWriter iterations;
    private PrintWriter islandNum;
    private PrintWriter isolatedNodes;
    private PrintWriter nodeLossOut;
    private PrintWriter nodeFlowOut;
    private PrintWriter nodeUtilizationOut;
    private PrintWriter linkOverloadOut;
    private PrintWriter nodeOverloadOut;

    public BenchmarkLogReplayer(String experimentSequenceNumber, int minLoad, int maxLoad, boolean writeToFile){
        this.expSeqNum=experimentSequenceNumber;
        this.writeToFile=writeToFile;
        this.expID=experimentToken+expSeqNum+"/";
        this.resultID=resultToken+"/"+expID+"/";
        
        this.replayer=new LogReplayer();
        this.loadLogs(peerletsLogToken+"/"+expID, minLoad, maxLoad);
        
        if(writeToFile)
            this.prepareResultOutput();
        
        this.replayResults();
        
        if(writeToFile)
            this.closeFiles();
    }

    public void loadLogs(String directory, int minLoad, int maxLoad){
        try{
            File folder = new File(directory);
            File[] listOfFiles = folder.listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()&&!listOfFiles[i].isHidden()) {
                    MeasurementLog loadedLog=replayer.loadLogFromFile(directory+listOfFiles[i].getName());
                    MeasurementLog replayedLog=this.getMemorySupportedLog(loadedLog, minLoad, maxLoad);
                    replayer.mergeLog(replayedLog);
                }
                else
                    if (listOfFiles[i].isDirectory()) {
                        //do sth else
                    }
            }
        }
        catch(IOException io){

        }
        catch(ClassNotFoundException ex){

        }
    }
    
    private void prepareResultOutput(){
        try{
            File resultLocation = new File(resultID);
            clearOutputFiles(resultLocation);
            resultLocation.mkdirs();
            
            allMetricsOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"allMetrics.txt", true)));
            // links
            linkLossOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"linkLoss.txt", true)));
            linkFlowOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"linkFlow.txt", true)));
            linkUtilizationOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"linkUtilization.txt", true)));
            linkOverloadOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"linkOverload.txt", true)));
            // nodes
            nodeLossOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"nodeLoss.txt", true)));
            nodeFlowOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"nodeFlow.txt", true)));
            nodeUtilizationOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"nodeUtilization.txt", true)));
            nodeOverloadOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"nodeOverload.txt", true)));
            epochPowerLossOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"nodeEpochPowLoss.txt", true)));
            totalPowerLossOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"nodeTotPowLoss.txt", true)));
            islandNum  = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"nodesIslands.txt", true)));
            isolatedNodes  = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"nodesIsolated.txt", true)));
            // system
            totalTimeOut = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"totalTime.txt", true)));
            iterations  = new PrintWriter(new BufferedWriter(new FileWriter(resultID+"iterations.txt", true)));
        }
        catch (IOException e) {
            logger.debug(e);
        }
    }
    
    
    private void closeFiles(){
        allMetricsOut.print("\n");
        // links
        linkLossOut.print("\n");
        linkFlowOut.print("\n");
        linkUtilizationOut.print("\n");
        linkOverloadOut.print("\n");
        // nodes
        nodeLossOut.print("\n");
        nodeFlowOut.print("\n");
        nodeUtilizationOut.print("\n");
        nodeOverloadOut.print("\n");
        epochPowerLossOut.print("\n");
        totalPowerLossOut.print("\n");
        islandNum.print("\n");
        isolatedNodes.print("\n");
        // system
        totalTimeOut.print("\n");
        iterations.print("\n");
        
        allMetricsOut.close();
        
        linkLossOut.close();
        linkFlowOut.close();
        linkUtilizationOut.close();
        linkOverloadOut.close();
        
        nodeLossOut.close();
        nodeFlowOut.close();
        nodeUtilizationOut.close();
        nodeOverloadOut.close();
        epochPowerLossOut.close();
        totalPowerLossOut.close();
        islandNum.close();
        isolatedNodes.close();
        
        totalTimeOut.close();
        iterations.close();
    }

    public void replayResults(){
        this.printGlobalMetricsTags();
//        this.calculatePeerResults(replayer.getCompleteLog());
        this.printLocalMetricsTags();
        replayer.replayTo(new MeasurementLoggerListener(){
            public void measurementEpochEnded(MeasurementLog log, int epochNumber){
                calculateEpochResults(log, epochNumber);
                calculateIterationResults(log, epochNumber);
            }
        });
    }

    private void calculatePeerResults(MeasurementLog globalLog){
        
    }

    private void calculateEpochResults(MeasurementLog log, Integer epochNumber){
        int totalIterations = (int) log.getAggregateByEpochNumber(epochNumber, Metrics.NEEDED_ITERATIONS).getMax();
        // links
        double avgLinkLosses = 1-(log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.ACTIVATED_LINKS).getSum()/log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.TOTAL_LINKS).getSum());
        double avgLinkFlow = log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.LINK_FLOW).getAverage();
        double avgLinkUtilization = log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.LINK_UTILIZATION).getAverage();
        double avgLinkOverload = 1-(log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.OVERLOADED_LINKS).getSum()/log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.TOTAL_LINKS).getSum());
        // nodes
        double avgNodeLosses = 1-(log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.ACTIVATED_NODES).getSum()/log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.TOTAL_NODES).getSum());
        double avgNodeFlow = log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.NODE_FLOW).getAverage();
        double avgNodeUtilization = log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.NODE_UTILIZATION).getAverage();
        double avgNodeOverload = 1-(log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.OVERLOADED_NODES).getSum()/log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.TOTAL_NODES).getSum());
        double relNodePowerLoss = 1.0-log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.NODE_FINAL_LOADING).getSum()/log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.NODE_INIT_LOADING).getSum();
        double relNodePowerLossSinceEpoch1 = 1.0-log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.NODE_FINAL_LOADING).getSum()/log.getAggregateByEpochNumber(1, 1, Metrics.NODE_INIT_LOADING).getSum();
        double islands = log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.ISLANDS).getMax();
        double isolNodes = log.getAggregateByEpochNumber(epochNumber, totalIterations, Metrics.ISOLATED_NODES).getMax();
        // system
        double simuTime = 0; // total time = sum(iteration times)
        for(Integer i=1; i<=totalIterations; i++)
            simuTime += log.getAggregateByEpochNumber(epochNumber, i, Metrics.TOT_SIMU_TIME).getSum(); 
        
        String delimiter = "";
        int length = 200;
        for(int i = 0; i<length; i++)
            delimiter = delimiter+"-";
        logger.info(String.format("%s\n",delimiter));
        logger.info(String.format("%10d%10s%20.2f%20.2f%20.2f%20.0f%20d%20.4f%20.4f%20.0f%20.0f\n",epochNumber,"total", avgLinkLosses, avgLinkFlow, avgLinkUtilization, simuTime, totalIterations, relNodePowerLoss, relNodePowerLossSinceEpoch1, islands, isolNodes));
        
        if(writeToFile){
            allMetricsOut.format("%s\n",delimiter);
            allMetricsOut.format("%10d%10s%20.2f%20.2f%20.2f%20.0f%20d%20.4f%20.4f%20.0f%20.0f\n",epochNumber,"total", avgLinkLosses, avgLinkFlow, avgLinkUtilization, simuTime, totalIterations, relNodePowerLoss, relNodePowerLossSinceEpoch1, islands, isolNodes);
        }
    }
    
    private void calculateIterationResults(MeasurementLog log, int epochNumber){
        int totalIterations = (int) log.getAggregateByEpochNumber(epochNumber, Metrics.NEEDED_ITERATIONS).getMax();
        
        for(int i=1; i<=totalIterations; i++){
            // links
            double avgLinkLosses = 1.0-(log.getAggregateByEpochNumber(epochNumber, i, Metrics.ACTIVATED_LINKS).getSum()/log.getAggregateByEpochNumber(epochNumber, i, Metrics.TOTAL_LINKS).getSum());
            double avgLinkFlow = log.getAggregateByEpochNumber(epochNumber, i, Metrics.LINK_FLOW).getAverage();
            double avgLinkUtilization = log.getAggregateByEpochNumber(epochNumber, i, Metrics.LINK_UTILIZATION).getAverage();
            double avgLinkOverload = log.getAggregateByEpochNumber(epochNumber, i, Metrics.OVERLOADED_LINKS).getSum()/log.getAggregateByEpochNumber(epochNumber, i, Metrics.TOTAL_LINKS).getSum();
            // nodes
            double avgNodeLosses = 1.0-(log.getAggregateByEpochNumber(epochNumber, i, Metrics.ACTIVATED_NODES).getSum()/log.getAggregateByEpochNumber(epochNumber, i, Metrics.TOTAL_NODES).getSum());
            double avgNodeFlow = log.getAggregateByEpochNumber(epochNumber, i, Metrics.NODE_FLOW).getAverage();
            double avgNodeUtilization = log.getAggregateByEpochNumber(epochNumber, i, Metrics.NODE_UTILIZATION).getAverage();
            double avgNodeOverload = log.getAggregateByEpochNumber(epochNumber, i, Metrics.OVERLOADED_NODES).getSum()/log.getAggregateByEpochNumber(epochNumber, i, Metrics.TOTAL_NODES).getSum();
            double relNodePowerLoss = 1.0-log.getAggregateByEpochNumber(epochNumber, i, Metrics.NODE_FINAL_LOADING).getSum()/log.getAggregateByEpochNumber(epochNumber, i, Metrics.NODE_INIT_LOADING).getSum();
            double relNodePowerLossSinceEpoch1 = 1.0-log.getAggregateByEpochNumber(epochNumber, i, Metrics.NODE_FINAL_LOADING).getSum()/log.getAggregateByEpochNumber(1, 1, Metrics.NODE_INIT_LOADING).getSum();
            double islands = log.getAggregateByEpochNumber(epochNumber, i, Metrics.ISLANDS).getMax();
            double isolNodes = log.getAggregateByEpochNumber(epochNumber, i, Metrics.ISOLATED_NODES).getMax();
            // system
            double simuTime = log.getAggregateByEpochNumber(epochNumber, i, Metrics.TOT_SIMU_TIME).getSum(); // This metric measures time of each iteration => total time = sum(iteration times)

            logger.info(String.format("%10s%10d%20.2f%20.2f%20.2f%20.0f%20s%20.4f%20.4f%20.0f%20.0f\n","", i, avgLinkLosses, avgLinkFlow, avgLinkUtilization, simuTime, "-", relNodePowerLoss, relNodePowerLossSinceEpoch1, islands, isolNodes));            
            
            if(writeToFile){
                allMetricsOut.format("%10s%10d%20.2f%20.2f%20.2f%20.0f%20s%20.4f%20.4f%20.0f%20.0f\n","", i, avgLinkLosses, avgLinkFlow, avgLinkUtilization, simuTime, "-", relNodePowerLoss, relNodePowerLossSinceEpoch1, islands, isolNodes);
                
                linkLossOut.print(avgLinkLosses);
                linkFlowOut.print(avgLinkFlow);
                linkUtilizationOut.print(avgLinkUtilization);
                linkOverloadOut.print(avgLinkOverload);
                
                nodeLossOut.print(avgNodeLosses);
                nodeFlowOut.print(avgNodeFlow);
                nodeUtilizationOut.print(avgNodeUtilization);
                nodeOverloadOut.print(avgNodeOverload);
                epochPowerLossOut.print(relNodePowerLoss);
                totalPowerLossOut.print(relNodePowerLossSinceEpoch1);
                islandNum.print(islands);
                isolatedNodes.print(isolNodes);
                
                totalTimeOut.print(simuTime);
                
                if(i!=totalIterations){
                    linkLossOut.print(coma);
                    linkFlowOut.print(coma);
                    linkUtilizationOut.print(coma);
                    linkOverloadOut.print(coma);
                    
                    nodeLossOut.print(coma);
                    nodeFlowOut.print(coma);
                    nodeUtilizationOut.print(coma);
                    nodeOverloadOut.print(coma);
                    epochPowerLossOut.print(coma);
                    totalPowerLossOut.print(coma);
                    islandNum.print(coma);
                    isolatedNodes.print(coma);
                    
                    totalTimeOut.print(coma);

                }
            }
        }   
        if(writeToFile){
            linkLossOut.print("\n");
            linkFlowOut.print("\n");
            linkUtilizationOut.print("\n");
            linkOverloadOut.print("\n");
            
            nodeLossOut.print("\n");
            nodeFlowOut.print("\n");
            nodeUtilizationOut.print("\n");
            nodeOverloadOut.print("\n");
            
            epochPowerLossOut.print("\n");
            totalPowerLossOut.print("\n");
            islandNum.print("\n");
            isolatedNodes.print("\n");
            
            totalTimeOut.print("\n");
            iterations.print(totalIterations + "\n");
        }
    }
    
    private MeasurementLog getMemorySupportedLog(MeasurementLog log, int minLoad, int maxLoad){
        return log.getSubLog(minLoad, maxLoad);
    }

    public void printGlobalMetricsTags(){
        logger.info("*** RESULTS PER PEER ***\n");
    }

    public void printLocalMetricsTags(){
        logger.info(String.format("*** RESULTS PER TIME STEP AND ITERATION  FOR EXPERIMENT %s ***\n", expID));
        logger.info(String.format("%10s%10s%20s%20s%20s%20s%20s%20s%20s%20s%20s\n", "TIME STEP","ITERATION","AVG links failed","AVG link flow","AVG link utilization","Simu Time [ms]", "# iterations", "Node pow Loss epoch", "Node Pow Loss total", "# islands", "# isolated nodes"));
        if(writeToFile){
            allMetricsOut.format("*** RESULTS PER TIME STEP AND ITERATION FOR EXPERIMENT %s ***\n", expID);
            allMetricsOut.format("%10s%10s%20s%20s%20s%20s%20s%20s%20s%20s%20s\n", "TIME STEP","ITERATION","AVG links failed","AVG link flow","AVG link utilization","Simu Time [ms]", "# iterations", "Node pow Loss epoch", "Node Pow Loss total", "# islands", "# isolated nodes");
        }
    }
    
    private static void clearOutputFiles(File experiment){
        File[] files = experiment.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    clearOutputFiles(f);
                } else {
                    f.delete();
                }
            }
        }
        experiment.delete();
    }
}
