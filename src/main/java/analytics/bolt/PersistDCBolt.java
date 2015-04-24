package analytics.bolt;

import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.MemberDCDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PersistDCBolt extends EnvironmentBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistTraitsBolt.class);
    private MemberDCDao memberDCDao;
	private OutputCollector outputCollector;
	
	public PersistDCBolt(String systemProperty){
	 super(systemProperty);
	 }
 
	@Override
	public void execute(Tuple input) {
		LOGGER.debug("Persisting DC in mongo");
		redisCountIncr("incoming_record");
		String l_id = input.getString(0);
		try {
			JSONObject obj = new JSONObject(input.getString(1));
			memberDCDao.addDateDC(l_id, obj.toString());
    		redisCountIncr("persisted_dc");
    		outputCollector.ack(input);
		} catch (JSONException e) {
			LOGGER.error("unable to persist dc",e);
    		redisCountIncr("persist_failed");
    		outputCollector.fail(input);
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
			memberDCDao = new MemberDCDao();
			this.outputCollector = collector;
			
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
	}

}
