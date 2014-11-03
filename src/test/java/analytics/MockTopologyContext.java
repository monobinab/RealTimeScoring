package analytics;

import java.util.List;
import java.util.Map;

import clojure.lang.Atom;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

public class MockTopologyContext extends TopologyContext{

	public MockTopologyContext(){
		super(null, null, null, null,
				null, null, null, null, null,
				null, null, null, null, null,
				null, null);
	}
	public MockTopologyContext(StormTopology topology, Map stormConf,
			Map<Integer, String> taskToComponent,
			Map<String, List<Integer>> componentToSortedTasks,
			Map<String, Map<String, Fields>> componentToStreamToFields,
			String stormId, String codeDir, String pidDir, Integer taskId,
			Integer workerPort, List<Integer> workerTasks,
			Map<String, Object> defaultResources,
			Map<String, Object> userResources,
			Map<String, Object> executorData, Map registeredMetrics,
			Atom openOrPrepareWasCalled) {
		super(topology, stormConf, taskToComponent, componentToSortedTasks,
				componentToStreamToFields, stormId, codeDir, pidDir, taskId,
				workerPort, workerTasks, defaultResources, userResources, executorData,
				registeredMetrics, openOrPrepareWasCalled);
		// TODO Auto-generated constructor stub
	}
	
    public IMetric registerMetric(String metricsName, IMetric metric, int interval){
		return new MultiCountMetric();
    	
    }

}
