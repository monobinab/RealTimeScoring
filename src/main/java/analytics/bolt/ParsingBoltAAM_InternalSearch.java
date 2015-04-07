package analytics.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import analytics.util.HostPortUtility;
import analytics.util.MongoNameConstants;
import analytics.util.dao.DivLnBoostDao;
import analytics.util.dao.DivLnVariableDao;
import analytics.util.dao.PidDivLnDao;
import analytics.util.objects.DivLn;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ParsingBoltAAM_InternalSearch extends ParseAAMFeeds {
	/**
	 * Created by Rock Wasserman 6/24/2014
	 */

    private Map<String, List<String>> divLnVariablesMap;
	private DivLnVariableDao divLnVariableDao;
	private PidDivLnDao pidDivLnDao;

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	public ParsingBoltAAM_InternalSearch (String systemProperty, String topic) {
		super(systemProperty, topic);
	
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
	/*
	 * (non-Javadoc)
	 *
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	//	HostPortUtility.getInstance(stormConf.get("nimbus.host").toString());
		divLnVariableDao = new DivLnVariableDao();
		pidDivLnDao = new PidDivLnDao();
		sourceTopic = "InternalSearch";

        // populate divLnVariablesMap
        divLnVariablesMap = divLnVariableDao.getDivLnVariable();
        
    }



    private String[] splitKeyWords(String keyWords) {
        String split[]=StringUtils.split(keyWords,"+");
        
        if(split !=null && split.length>0) {
			return split;
		}
		else {
			return null;
		}
	}


    private boolean isJSONValid(String test) {
    	 
        try {
            new JSONObject(test);
            return true;
        } catch (JSONException ex) {
            try {
                new JSONArray(test);
                return true;
            } catch (JSONException ex1) {
                //System.out.println(test);
                return false;
            }
        }
    }
        	
	/*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
     * topology.OutputFieldsDeclarer)
     */

	@Override
	protected Map<String, String> processList(String current_l_id) {
	    	
    	String queryResultsDoc = new String();
    	Set<String> pidSet = new HashSet<String>();
    	Collection<String> searchStringsCollection = l_idToValueCollectionMap.get(current_l_id);
    	
    	if(searchStringsCollection==null || searchStringsCollection.isEmpty()|| (searchStringsCollection.toArray())[0].toString().trim().equalsIgnoreCase(""))
    		return null;
    	
    	for(String searchString : searchStringsCollection) {
	    	String[] search = splitKeyWords(searchString);
	    	
	    	//CONSTRUCT URL - queries Solr
			String URL1 = "http://solrx308p.stress.ch3.s.com:8180/search/select?qt=search&wt=json&q=";
			String URL2 = "&start=0&rows=50&fq=catalogs:%28%2212605%22%29&sort=instock%20desc,score%20desc,revenue%20desc&sortPrefix=L6;S4;10153&globalPrefix=L6,S4,10153&spuAvailability=S4&lmpAvailability=L6&fvCutoff=22&fqx=!%28storeAttributes:%28%2210153_DEFAULT_FULFILLMENT=SPU%22%29%20AND%20storeOrigin:%28%22Kmart%22%29%29&site=prod";
			String query = new String();
			
			query = URL1;
			 StringBuilder sb_query = new StringBuilder(query);
			int countKeyWords=0;
			for(String keyWord:search) {
				if(!keyWord.equalsIgnoreCase("N/A")) {
					countKeyWords++;
					if(countKeyWords==1) {
						sb_query.append(keyWord);
					}
					else {
						sb_query.append("%20");
						sb_query.append(keyWord);
					}
				}
			}
			query = sb_query.toString();
			query = query + URL2;
			
			if(countKeyWords>0) {
			
				try {
					//System.out.println(query);
					try {
						TimeUnit.MILLISECONDS.sleep(100);
					} catch (InterruptedException e) {
						LOGGER.debug("Unable to wait",e);
					}
					long t1 = System.currentTimeMillis();
					Document doc = Jsoup.connect(query).get();
					long t2 = System.currentTimeMillis() - t1;
					LOGGER.debug(" @@@ Query time: " + t2);
					doc.body().wrap("<pre></pre>");
					String text = doc.text();
					// Converting nbsp entities
					text = text.replaceAll("\u00A0", " ");
					
					queryResultsDoc = text;
				} catch (IOException e) {
					LOGGER.debug("Unable to process keywords",e);
				}
				if(queryResultsDoc==null) {
					LOGGER.debug("query results null");
				}
				
				else {
					if(isJSONValid(queryResultsDoc)) {
						if(new JsonParser().parse(queryResultsDoc).isJsonObject()) {
							JsonObject queryResultsToJson = new JsonParser().parse(queryResultsDoc).getAsJsonObject();
							if(queryResultsToJson.get("response")!=null && queryResultsToJson.get("response").isJsonObject()) {
								JsonObject response = queryResultsToJson.get("response").getAsJsonObject();
								if(response.get("docs").isJsonArray()) {
									JsonArray docs = response.getAsJsonArray("docs").getAsJsonArray();
									for(JsonElement doc:docs){
										if(doc.isJsonObject() && doc.getAsJsonObject().get("partnumber") != null) {
											pidSet.add(doc.getAsJsonObject().get("partnumber").toString().replace("\"", ""));
										}
									}
								}
							}
						}
					}
				}
			}
    	}
    	if(pidSet.isEmpty()) {
    		return new HashMap<String,String>();
    	}
    	
    	Map<String,String> variableValueMap = new HashMap<String,String>();
    	
    	for(String pid: pidSet) {
    		DivLn divLnObj = pidDivLnDao.getDivLnFromPid(pid);
    		if(divLnObj != null) {
	    		String div = divLnObj.getDiv();
	    		String divLn = divLnObj.getDivLn();
	    		Collection<String> var = new ArrayList<String>();
	    		if(divLnVariablesMap.containsKey(div)) {
	    			var = divLnVariablesMap.get(div);
	    			for(String v:var) {
		    			if(variableValueMap.containsKey(var)) {
		    				int value = 1 + Integer.valueOf(variableValueMap.get(v));
		    				variableValueMap.remove(v);
		    				variableValueMap.put(v, String.valueOf(value));
		    			}
		    			else {
		    				variableValueMap.put(v, "1");
		    			}
	    			}
	    		}
	    		if(divLnVariablesMap.containsKey(divLn)) {
	    			var = divLnVariablesMap.get(divLn);
	    			for(String v:var) {
		    			if(variableValueMap.containsKey(var)) {
		    				int value = 1 + Integer.valueOf(variableValueMap.get(v));
		    				variableValueMap.remove(v);
		    				variableValueMap.put(v, String.valueOf(value));
		    			}
		    			else {
		    				variableValueMap.put(v, "1");
		    			}
	    			}
	    		}
    		}
    	}
    	
    	return variableValueMap;
    }
	@Override
	protected String[] splitRec(String webRec) {
		//TODO: See if other fields in the record are relevant. It was anyway not being used, so made this change
	        //System.out.println("WEB RECORD: " + webRec);
		webRec = webRec.replaceAll("[']",""); ;
	        String split[]=StringUtils.split(webRec,",");
	       
	        if(split !=null && split.length>0) {
	            return new String [] { split[1], split[2] };
			}
			else {
				return null;
			}
		}
    
}
