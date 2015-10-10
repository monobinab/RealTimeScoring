package analytics.bolt;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class BrowseCountPersistBolt extends EnvironmentBolt{
	
	static final Logger LOGGER = LoggerFactory
			.getLogger(BrowseCountPersistBolt.class);
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;
	Map<String, String> sourceMap = new HashMap<String, String>();
	private String systemProperty;
	public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
	
	public BrowseCountPersistBolt(String systemProperty) {
		this.systemProperty = systemProperty;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        super.prepare(stormConf, context, collector);
        sourceMap.put("Browse", "PR");
	}
	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		Map<String, String> tagsMap = JsonUtils.restoreTagsListFromJson(input.getString(1));
		System.out.println(tagsMap);
		
		
	}
}
