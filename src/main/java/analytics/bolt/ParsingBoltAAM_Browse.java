package analytics.bolt;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import analytics.util.PidMatchUtils;
import analytics.util.objects.DivLn;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

public class ParsingBoltAAM_Browse extends ParseAAMFeeds {
	private static final long serialVersionUID = 1L;
	private PidMatchUtils pidMatchUtil;

	public ParsingBoltAAM_Browse(String systemProperty, String source) {
		super(systemProperty, source);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		pidMatchUtil = new PidMatchUtils();
	}

	@Override
	protected Map<String, String> processList(String current_l_id,
			 Map<String, Collection<String>> l_idToCurrentPidCollectionMap) {
	
		Map<String, String> incomingModelCodeMap = new HashMap<String, String>();

		Collection<String> currentPidsCollection = l_idToCurrentPidCollectionMap
				.get(current_l_id);

		if (currentPidsCollection == null
				|| currentPidsCollection.isEmpty()
				|| (currentPidsCollection.toArray())[0].toString().trim()
						.equalsIgnoreCase(""))
			return null;

		LOGGER.info(current_l_id + " has " + currentPidsCollection.size() + " pids");
		//System.out.println(current_l_id + " has " + currentPidsCollection.size() +  " pids");
		for (String pid : currentPidsCollection) {
			// query MongoDB for division and line associated with the pid
			DivLn divLnObj = pidMatchUtil.getDivInformation(pid);
			if (divLnObj == null) {
				LOGGER.info("No Div Info found for Pid : " + pid);
				continue;
			}
			
			String div = divLnObj.getDiv();
			String divLn = divLnObj.getDivLn();
			
			
			/*
			 * populating incomingModelcodeMap from divLn
			 */
			getIncomingModelCodeMap(div, incomingModelCodeMap);
			getIncomingModelCodeMap(divLn, incomingModelCodeMap);
			
		}
			return incomingModelCodeMap;
	}

	@Override
	protected String[] splitRec(String webRec) {
		webRec = webRec.replaceAll("[']", "");
		String split[] = StringUtils.split(webRec, ",");

		if (split != null && split.length > 0) {
			return split;
		} else {
			return null;
		}
	}
}
