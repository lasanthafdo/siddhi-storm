package org.wso2.siddhi.storm.scheduler;

import backtype.storm.scheduler.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;


public class SiddhiStormScheduler implements IScheduler{

    public static final String SIDDHI_TOPOLOGY_NAME = "LatencyMeasureTopology";
    private static Log log = LogFactory.getLog(SiddhiStormScheduler.class);
    Map conf;
    Map<String, SupervisorDetails> componentToSupervisor = new HashMap<String, SupervisorDetails>();

    public void prepare(Map conf) {
        this.conf = conf;
    }

    private void indexSupervisors(Collection<SupervisorDetails> supervisorDetails){
        for (SupervisorDetails supervisor : supervisorDetails) {
            Map meta = (Map) supervisor.getSchedulerMeta();
            String componentId = (String) meta.get("dedicated.to.component");

            if (componentId != null && !componentId.isEmpty()){
                componentToSupervisor.put(componentId, supervisor);
            }
        }
    }

    private void freeAllSlots(SupervisorDetails supervisor, Cluster cluster){
        for (Integer port : cluster.getUsedPorts(supervisor)) {
            cluster.freeSlot(new WorkerSlot(supervisor.getId(), port));
        }
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        log.info("Scheduling by Siddhi Storm Scheduler");
        TopologyDetails siddhiTopology = topologies.getByName(SIDDHI_TOPOLOGY_NAME);

        if (siddhiTopology != null){
            boolean isSchedulingNeeded = cluster.needsScheduling(siddhiTopology);

            if (!isSchedulingNeeded){
                log.info(SIDDHI_TOPOLOGY_NAME + " already scheduled!");
            }else{
                log.info("Scheduling "+ SIDDHI_TOPOLOGY_NAME);

                indexSupervisors(cluster.getSupervisors().values());

                Map<String, List<ExecutorDetails>> componentToExecutors =
                        cluster.getNeedsSchedulingComponentToExecutors(siddhiTopology);

                for(Map.Entry<String, List<ExecutorDetails>> entry : componentToExecutors.entrySet()){
                    String componentName = entry.getKey();
                    List<ExecutorDetails> executors = entry.getValue();
                    SupervisorDetails supervisor = componentToSupervisor.get(componentName);

                    if (supervisor != null){
                        log.info("Scheduling " + componentName + " on " + supervisor.getHost());
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);

                        if (availableSlots.isEmpty()){
                            throw new RuntimeException("No free slots available for scheduling in dedicated supervisor @ " + supervisor.getHost());
                        }

                        int availableSlotCount = availableSlots.size();
                        int executorCount = executors.size();

                        List<List<ExecutorDetails>> executorsForSlots = new ArrayList<List<ExecutorDetails>>();
                        for (int i = 0; i < availableSlotCount; i++){
                            executorsForSlots.add(new ArrayList<ExecutorDetails>());
                        }

                        for (int i = 0; i < executorCount; i++){
                            int slotToAllocate = i % availableSlotCount;
                            ExecutorDetails executor = executors.get(i);
                            executorsForSlots.get(slotToAllocate).add(executor);
                        }

                        for (int i = 0; i < availableSlotCount; i++){
                            if (!executorsForSlots.get(i).isEmpty()){
                                cluster.assign(availableSlots.get(i), siddhiTopology.getId(), executorsForSlots.get(i));
                            }
                        }
                    }else{
                        log.info("No dedicated supervisor for " + componentName);
                    }
                }
            }
        }
        new EvenScheduler().schedule(topologies, cluster);
    }
}


