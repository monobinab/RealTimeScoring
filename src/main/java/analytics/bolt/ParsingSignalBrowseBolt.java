package analytics.bolt;

import static backtype.storm.utils.Utils.tuple;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.MemberLoyIdDao;
import analytics.util.dao.MemberLoyIdDaoImpl;
import analytics.util.objects.RtsCommonObj;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ParsingSignalBrowseBolt extends EnvironmentBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingSignalBrowseBolt.class);
	private OutputCollector outputCollector;
	private MemberLoyIdDao memberLoyIdDao;
	
	 public ParsingSignalBrowseBolt(String systemProperty){
		 super(systemProperty);
	 }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		memberLoyIdDao = new MemberLoyIdDaoImpl();
	}
	
	@Override
	public void execute(Tuple input) {
		LOGGER.info("~~~~~~~~~~Incoming tuple in SignalBrowseBolt: " + input);
		
		redisCountIncr("incoming_tuples");
		RtsCommonObj rtsCommonObj = (RtsCommonObj) input.getValueByField("rtsCommonObj");
		String lId = null;
		ArrayList<String> pidLst = null;

		try {
				lId = rtsCommonObj.getLyl_id_no();
				pidLst = rtsCommonObj.getPidList();
				LOGGER.info("PROCESSING L_Id: " + lId +" with PIDs " + pidLst);
				String loyaltyId = memberLoyIdDao.getLoyaltyId(lId);
						
				if(lId != null && pidLst != null && pidLst.size()>0){
					ArrayList<String> lst = new ArrayList<String>(pidLst.subList(1, pidLst.size()));
					String pidLstStr = StringUtils.join(lst, ',');
				//	String str = "[,"+loyaltyId+","+pidLstStr+",]";
					String str = loyaltyId+","+pidLstStr;
					outputCollector.emit(tuple(str));
					str = null;
					pidLstStr = null;
					lst = null;
					
				}else
					LOGGER.info("Either L_Id is null or Pid List is null. No sending to Parsing Bolt .. Input Tuple : " +input);
			
			redisCountIncr("success_signal_browse");
			outputCollector.ack(input);
		} catch (Exception e) {
			LOGGER.error("Exception Occured at SignalBrowseBolt for Lid" + lId );
			e.printStackTrace();
			
			redisCountIncr("failure_signal_browse");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}