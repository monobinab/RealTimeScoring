package analytics.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class TestingSpout extends BaseRichSpout{

	private SpoutOutputCollector outputCollector;
	
	BufferedReader bufferedReader = null;
	FileReader fileReader = null;
	 List<String> lids = null;
	public TestingSpout(){
		
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.outputCollector = collector;
		 String fileName = "C:/Users/kmuthuk/Desktop/l_ids.txt";
		 String line = null;
		 lids = new ArrayList<String>();
         try {
                fileReader = 
                new FileReader(fileName);

            bufferedReader = 
                new BufferedReader(fileReader);
            
        	while((line = bufferedReader.readLine()) != null) {
        		lids.add(line);
        	}
        	
        	bufferedReader.close(); 
        }
        catch(Exception e){
        	e.printStackTrace();
        }
	}
	

	@Override
	public void nextTuple() {
	//	System.out.println(lids.size());
		
	//	String line = null;
		if(lids != null){
		 try {
		//	while((line = bufferedReader.readLine()) != null) {
			 for(int i=0; i<lids.size(); i++){
				Map<String, String> varaibleValueMap = new HashMap<String, String>();
				varaibleValueMap.put("BLACKOUT_REGRIG", "0.0");
				List<Object> listToEmit = new ArrayList<Object>();
				String l_id = SecurityUtils.hashLoyaltyId(lids.get(i));
				 listToEmit.add(l_id);
		         listToEmit.add(JsonUtils.createJsonFromStringStringMap(varaibleValueMap));
		         listToEmit.add("NPOS");
		        listToEmit.add(lids.get(i));
		       System.out.println(l_id + ", " + lids.get(i) + ", " + varaibleValueMap);
		       this.outputCollector.emit(listToEmit);
			 }
			 
			 lids = null;
		//	 bufferedReader.close(); 
			System.out.println("DONE~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		 
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source","lyl_id_no"));
		
	}
}
