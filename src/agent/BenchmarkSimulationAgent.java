/*
 * Copyright (C) 2015 SFINA Team
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package agent;

import agents.simulation.SimulationAgent;
import java.util.HashMap;
import java.util.HashSet;
import network.Link;
import network.Node;
import org.apache.log4j.Logger;
import protopeer.measurement.MeasurementFileDumper;
import protopeer.measurement.MeasurementLog;
import protopeer.measurement.MeasurementLoggerListener;

/**
 * General domain independent measurements. 
 * Features:
 * - Logs per iteration and per time step
 * - Offers overridable methods to easily log more values
 * - Plays together with BenchmarkLogReplayer to compute, output and display logging results
 * 
 * @author Ben
 */
public class BenchmarkSimulationAgent extends SimulationAgent{
    
    private static final Logger logger = Logger.getLogger(BenchmarkSimulationAgent.class);
    
    // Time, Iteration, Node/Link ID, Metric, Values
    private HashMap<Integer,HashMap<Integer,HashMap<String,HashMap<Metrics,Object>>>> temporalLinkMetrics;
    private HashMap<Integer,HashMap<Integer,HashMap<String,HashMap<Metrics,Object>>>> temporalNodeMetrics;
    
    // Time, Iteration, Metric, Values
    private HashMap<Integer,HashMap<Integer,HashMap<Metrics,Object>>> temporalSystemMetrics;
    private long simulationStartTime;
    
    public BenchmarkSimulationAgent(String experimentID){
        super(experimentID);
        this.temporalLinkMetrics=new HashMap();
        this.temporalNodeMetrics=new HashMap();
        this.temporalSystemMetrics=new HashMap();
    }
    
    public void initMeasurementVariables(){
        if(getIteration()==1){
            this.getTemporalLinkMetrics().put(this.getSimulationTime(), new HashMap<>());
            this.getTemporalNodeMetrics().put(this.getSimulationTime(), new HashMap<>());
            this.getTemporalSystemMetrics().put(this.getSimulationTime(), new HashMap<>());
        }

        HashMap<String,HashMap<Metrics,Object>> linkMetrics=new HashMap<>();
        for(Link link:this.getFlowNetwork().getLinks()){
            HashMap<Metrics,Object> metrics=new HashMap<>();
            linkMetrics.put(link.getIndex(), metrics);
        }
        this.getTemporalLinkMetrics().get(this.getSimulationTime()).put(this.getIteration(), linkMetrics);
        
        HashMap<String,HashMap<Metrics,Object>> nodeMetrics=new HashMap<>();
        for(Node node:this.getFlowNetwork().getNodes()){
            HashMap<Metrics,Object> metrics=new HashMap<>();
            nodeMetrics.put(node.getIndex(), metrics);
        }
        this.getTemporalNodeMetrics().get(this.getSimulationTime()).put(this.getIteration(), nodeMetrics);
        
        this.getTemporalSystemMetrics().get(this.getSimulationTime()).put(this.getIteration(), new HashMap<>());
    }
    
    public void calculateTotalNumber(){
        for(Link link:this.getFlowNetwork().getLinks()){
            HashMap<Metrics,Object> metrics=this.getTemporalLinkMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(link.getIndex());
            metrics.put(Metrics.TOTAL_LINKS, 1.0);
        }
        for(Node node:this.getFlowNetwork().getNodes()){
            HashMap<Metrics,Object> metrics=this.getTemporalNodeMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(node.getIndex());
            metrics.put(Metrics.TOTAL_NODES, 1.0);
        }
    }
    
    public void calculateActivationStatus(){
        for(Link link:this.getFlowNetwork().getLinks()){
            boolean activationStatus=link.isActivated();
            HashMap<Metrics,Object> metrics=this.getTemporalLinkMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(link.getIndex());
            metrics.put(Metrics.ACTIVATED_LINKS, (activationStatus==true) ? 1.0 : 0.0);
        }
        for(Node node:this.getFlowNetwork().getNodes()){
            boolean activationStatus=node.isActivated();
            HashMap<Metrics,Object> metrics=this.getTemporalNodeMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(node.getIndex());
            metrics.put(Metrics.ACTIVATED_NODES, (activationStatus==true) ? 1.0 : 0.0);
        }
    }
    
    public void calculateFlow(){
        for(Link link:this.getFlowNetwork().getLinks()){
            double flow=link.getFlow();
            HashMap<Metrics,Object> metrics=this.getTemporalLinkMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(link.getIndex());
            metrics.put(Metrics.LINK_FLOW, (link.isActivated()) ? flow : 0.0);
        }
        for(Node node:this.getFlowNetwork().getNodes()){
            double flow=node.getFlow();
            HashMap<Metrics,Object> metrics=this.getTemporalNodeMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(node.getIndex());
            metrics.put(Metrics.NODE_FLOW, (node.isActivated()) ? flow : 0.0);
        }
    }
    
    public void calculateUtilization(){
        for(Link link:this.getFlowNetwork().getLinks()){
            double flow=link.getFlow();
            double capacity=link.getCapacity();
            double utilization=flow/capacity;
            HashMap<Metrics,Object> metrics=this.getTemporalLinkMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(link.getIndex());
            metrics.put(Metrics.LINK_UTILIZATION, (link.isActivated()) ? utilization : 1.0);
        }
        for(Node node:this.getFlowNetwork().getNodes()){
            double flow=node.getFlow();
            double capacity=node.getCapacity();
            double utilization=flow/capacity;
            HashMap<Metrics,Object> metrics=this.getTemporalNodeMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(node.getIndex());
            metrics.put(Metrics.NODE_UTILIZATION, (node.isActivated()) ? utilization : 1.0);
        }
    }
    
    public void calculateOverloadStatus(){
        for(Link link:this.getFlowNetwork().getLinks()){
            boolean overloaded = Math.abs(link.getFlow()) > Math.abs(link.getCapacity());
            HashMap<Metrics,Object> metrics=this.getTemporalLinkMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(link.getIndex());
            metrics.put(Metrics.OVERLOADED_LINKS, overloaded ? 1.0 : 0.0);
        }
        for(Node node:this.getFlowNetwork().getNodes()){
            boolean overloaded = Math.abs(node.getFlow()) > Math.abs(node.getCapacity());
            HashMap<Metrics,Object> metrics=this.getTemporalNodeMetrics().get(this.getSimulationTime()).get(this.getIteration()).get(node.getIndex());
            metrics.put(Metrics.OVERLOADED_NODES, overloaded ? 1.0 : 0.0);
        }
    }
    
    public void saveStartTime(){
        this.simulationStartTime = System.currentTimeMillis();
    }
    
    public void saveSimuTime(){
        double totSimuTime = System.currentTimeMillis() - simulationStartTime;
        this.getTemporalSystemMetrics().get(this.getSimulationTime()).get(this.getIteration()).put(Metrics.TOT_SIMU_TIME, totSimuTime);
    }
    
    public void saveIterationNumber(){
        int iter = getIteration();
        this.getTemporalSystemMetrics().get(this.getSimulationTime()).get(this.getIteration()).put(Metrics.NEEDED_ITERATIONS, iter);
    }
    
    @Override
    public void runInitialOperations(){
        this.initMeasurementVariables();
        this.saveStartTime();
    }
    
    @Override
    public void runFinalOperations(){
        this.calculateActivationStatus();
        this.calculateFlow();
        this.calculateUtilization();
        this.calculateTotalNumber();
        this.calculateOverloadStatus();
        this.saveSimuTime();
        this.saveIterationNumber();
    }
        
    /**
     * @return the temporalLinkMetrics
     */
    public HashMap<Integer,HashMap<Integer,HashMap<String,HashMap<Metrics,Object>>>> getTemporalLinkMetrics() {
        return temporalLinkMetrics;
    }

    /**
     * @return the temporalNodeMetrics
     */
    public HashMap<Integer,HashMap<Integer,HashMap<String,HashMap<Metrics,Object>>>> getTemporalNodeMetrics() {
        return temporalNodeMetrics;
    }
    
    /**
     * 
     * @return the temporalSystemMetrics
     */
    public HashMap<Integer,HashMap<Integer,HashMap<Metrics,Object>>> getTemporalSystemMetrics() {
        return temporalSystemMetrics;
    }
    
    // *************************** Measurements ******************************** /
    // at end of simulation step
    // ************************************************************************* /
    
    /**
     * Scheduling the measurements for the simulation agent
     */
    @Override
    public void scheduleMeasurements(){
        setMeasurementDumper(new MeasurementFileDumper(getPeersLogDirectory()+this.getExperimentID()+this.getPeerTokenName()));
        getPeer().getMeasurementLogger().addMeasurementLoggerListener(new MeasurementLoggerListener(){
            public void measurementEpochEnded(MeasurementLog log, int epochNumber){
                int simulationTime=getSimulationTime();
                Integer totalIteration=getIteration();
                
                if(simulationTime>=1){
                    logger.debug("Logging at simulation time " + simulationTime);
                    log.logTagSet(simulationTime, new HashSet(getFlowNetwork().getLinks()), simulationTime);
                    log.logTagSet(simulationTime, new HashSet(getFlowNetwork().getNodes()), simulationTime);
                    
                    for(Integer iteration=1; iteration<=totalIteration; iteration++){
                        for(Link link:getFlowNetwork().getLinks()){
                            try {
                            HashMap<Metrics,Object> linkMetrics=getTemporalLinkMetrics().get(simulationTime).get(iteration).get(link.getIndex());
                            log.log(simulationTime, iteration, Metrics.LINK_UTILIZATION, ((Double)linkMetrics.get(Metrics.LINK_UTILIZATION)));
                            log.log(simulationTime, iteration, Metrics.LINK_FLOW, ((Double)linkMetrics.get(Metrics.LINK_FLOW)));
                            log.log(simulationTime, iteration, Metrics.ACTIVATED_LINKS, ((Double)linkMetrics.get(Metrics.ACTIVATED_LINKS)));
                            log.log(simulationTime, iteration, Metrics.OVERLOADED_LINKS, ((Double)linkMetrics.get(Metrics.OVERLOADED_LINKS)));
                            log.log(simulationTime, iteration, Metrics.TOTAL_LINKS, ((Double)linkMetrics.get(Metrics.TOTAL_LINKS)));
                            logLinkMetrics(log, simulationTime, iteration, linkMetrics);
                            }catch(Exception ex){
                                logger.info(" Exception in Benchmarksimulation Agent ");
                            }
                        }
                        for(Node node:getFlowNetwork().getNodes()){
                            try {
                            HashMap<Metrics,Object> nodeMetrics=getTemporalNodeMetrics().get(simulationTime).get(iteration).get(node.getIndex());
                            log.log(simulationTime, iteration, Metrics.NODE_UTILIZATION, ((Double)nodeMetrics.get(Metrics.NODE_UTILIZATION)));
                            log.log(simulationTime, iteration, Metrics.NODE_FLOW, ((Double)nodeMetrics.get(Metrics.NODE_FLOW)));
                            log.log(simulationTime, iteration, Metrics.ACTIVATED_NODES, ((Double)nodeMetrics.get(Metrics.ACTIVATED_NODES)));
                            log.log(simulationTime, iteration, Metrics.OVERLOADED_NODES, ((Double)nodeMetrics.get(Metrics.OVERLOADED_NODES)));
                            log.log(simulationTime, iteration, Metrics.TOTAL_NODES, ((Double)nodeMetrics.get(Metrics.TOTAL_NODES)));
                            logNodeMetrics(log, simulationTime, iteration, nodeMetrics);
                            }catch(Exception ex){
                                logger.info("Exception in BenchmarkSimulationAgent 2");
                            }
                        }
                        try {
                            HashMap<Metrics,Object> sysMetrics=getTemporalSystemMetrics().get(simulationTime).get(iteration);
                            log.log(simulationTime, iteration, Metrics.TOT_SIMU_TIME, ((Double)sysMetrics.get(Metrics.TOT_SIMU_TIME)));
                            log.log(simulationTime, Metrics.NEEDED_ITERATIONS, (Integer)sysMetrics.get(Metrics.NEEDED_ITERATIONS));
                            logSystemMetrics(log, simulationTime, iteration, sysMetrics);
                        }catch(Exception ex){
                            logger.info("Exception in BenchmarkSimulationAgent 3");
                        }
                    }
                }
                getMeasurementDumper().measurementEpochEnded(log, simulationTime);
                log.shrink(simulationTime, simulationTime+1);
            }
        });
    }
    
    /**
     * Override to log more link metrics.
     * @param log
     * @param simulationTime
     * @param iteration
     * @param linkMetrics 
     */
    public void logLinkMetrics(MeasurementLog log, int simulationTime, Integer iteration, HashMap<Metrics,Object> linkMetrics){
        //log.log(simulationTime, iteration, Metrics.TOTAL_LINES, ((Double)linkMetrics.get(Metrics.TOTAL_LINES)));

    }
    
    /**
     * Override to log more node metrics.
     * @param log
     * @param simulationTime
     * @param iteration
     * @param nodeMetrics 
     */
    public void logNodeMetrics(MeasurementLog log, int simulationTime, Integer iteration, HashMap<Metrics,Object> nodeMetrics){
        //log.log(simulationTime, iteration, Metrics.TOTAL_LINES, ((Double)linkMetrics.get(Metrics.TOTAL_LINES)));
    }
    
    /**
     * Override to log more system metrics.
     * @param log
     * @param simulationTime
     * @param iteration
     * @param sysMetrics 
     */
    public void logSystemMetrics(MeasurementLog log, int simulationTime, Integer iteration, HashMap<Metrics,Object> sysMetrics) {
        //log.log(simulationTime, iteration, Metrics.TOT_SIMU_TIME, ((Double)sysMetrics.get(Metrics.TOT_SIMU_TIME)));
    }
}
