package analytics.bolt;

import static backtype.storm.utils.Utils.tuple;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	 public ParsingSignalBrowseBolt(String systemProperty){
		 super(systemProperty);
	 }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		LOGGER.info("~~~~~~~~~~Incoming tuple in SignalBrowseBolt: " + input);
		
		redisCountIncr("incoming_tuples");
		RtsCommonObj rtsCommonObj = (RtsCommonObj) input.getValueByField("rtsCommonObj");
		String lyl_id_no = null;
		ArrayList<String> pidLst = null;

		try {
				lyl_id_no = rtsCommonObj.getLyl_id_no();
				pidLst = rtsCommonObj.getPidList();
				if(pidLst.get(0).length() != 13){
					pidLst.add(0, "");
					redisCountIncr("lids_proce_noTS");
				}
				LOGGER.info("PROCESSING L_Id: " + lyl_id_no +" with PIDs " + pidLst);
				
				
				if(lyl_id_no != null && pidLst != null && pidLst.size()>0){
					//String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
					
					ArrayList<String> lst = new ArrayList<String>(pidLst.subList(1, pidLst.size()));
					String pidLstStr = StringUtils.join(lst, ',');
					String str = "[,"+lyl_id_no+","+pidLstStr+",]";
					
					outputCollector.emit(tuple(str));
					
					str = null;
					pidLstStr = null;
					lst = null;
					
				}else
					LOGGER.info("Either L_Id is null or Pid List is null. No sending to Parsing Bolt .. Input Tuple : " +input);
			
			redisCountIncr("success_signal_browse");
			outputCollector.ack(input);
		} catch (Exception e) {
			LOGGER.error("Exception Occured at SignalBrowseBolt for Lid" + lyl_id_no );
			e.printStackTrace();
			
			redisCountIncr("failure_signal_browse");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
