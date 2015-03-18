package analytics.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.HostPortUtility;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberDCDao;
import analytics.util.dao.MemberTraitsDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PersistDCBolt extends BaseRichBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistTraitsBolt.class);
    private MemberDCDao memberDCDao;
	private MultiCountMetric countMetric;
	private OutputCollector outputCollector;

	@Override
	public void execute(Tuple input) {
		LOGGER.debug("Persisting DC in mongo");
		countMetric.scope("incoming_record").incr();
		String l_id = input.getString(0);
		try {
			JSONObject obj = new JSONObject(input.getString(1));
			memberDCDao.addDateDC(l_id, obj.toString());
    		countMetric.scope("persisted_dc").incr();
    		outputCollector.ack(input);
		} catch (JSONException e) {
			LOGGER.error("unable to persist dc",e);
    		countMetric.scope("persist_failed").incr();
    		outputCollector.fail(input);
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
		   HostPortUtility.getEnvironment(stormConf.get("nimbus.host").toString());
			memberDCDao = new MemberDCDao();
			this.outputCollector = collector;
			initMetrics(context);
		
	}
	void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
