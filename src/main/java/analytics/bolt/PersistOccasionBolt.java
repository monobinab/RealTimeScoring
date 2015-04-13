package analytics.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;
import analytics.util.HostPortUtility;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberMDTagsDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PersistOccasionBolt extends EnvironmentBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistOccasionBolt.class);
	private MemberMDTagsDao memberMDTagsDao;
	private MultiCountMetric countMetric;
	private OutputCollector outputCollector;
	 public PersistOccasionBolt(String systemProperty){
		 super(systemProperty);
	 }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		memberMDTagsDao = new MemberMDTagsDao();
		initMetrics(context);
	}
	void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }


	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
	//	System.out.println("IN PERSIST BOLT: " + input);
		LOGGER.info("~~~~~~~~~~Incoming tuple in PersistOccasionbolt: " + input);
		countMetric.scope("incoming_tuples").incr();
		List<String> tags = new ArrayList<String>();
		String l_id = input.getString(0);
		try {
			String tag = input.getString(1);
			if(tag != null && !tag.isEmpty()){
				String[] tagsArray = tag.split(",");
				tags = Arrays.asList(tagsArray);
				memberMDTagsDao.addMemberMDTags(l_id, tags);
				LOGGER.info("PERSIST OCCATION UPDATE: " + l_id + "~"+tags);
				countMetric.scope("persisted_occasionTags").incr();
			}
			else{
				memberMDTagsDao.deleteMemberMDTags(l_id);
				LOGGER.info("PERSIST OCCATION DELETE: " + l_id);
			}
			
			//Emit the tuples to scoring bolt only if there are changes to the Variables
			if(input.getString(2)!=null && input.getString(2).trim().length()>0){
				List<Object> listToEmit = new ArrayList<Object>();
					listToEmit.add(l_id);
					listToEmit.add(input.getString(2));
			    	listToEmit.add(input.getString(3));
			    	listToEmit.add(input.getString(4));
			    	outputCollector.emit(listToEmit);
			}
			outputCollector.ack(input);
		} catch (Exception e) {
			LOGGER.error("Json Exception ", e);
			countMetric.scope("persist_failed").incr();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("l_id", "lineItemAsJsonString","source","lyl_id_no"));
		}

}
