package analytics.bolt;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import analytics.util.SingletonJsonParser;
import analytics.util.dao.PidDivLnDao;
import analytics.util.objects.DivLn;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ParsingBoltAAM_InternalSearch extends ParseAAMFeeds {
	private static final long serialVersionUID = 1L;
	private PidDivLnDao pidDivLnDao;

	public ParsingBoltAAM_InternalSearch (String systemProperty, String topic) {
		super(systemProperty, topic);
	
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		pidDivLnDao = new PidDivLnDao();
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

   /*private boolean isJSONValid(String test) {
        try {
            JSONObject jsonObj = new JSONObject(test);
            jsonObj = null;
            return true;
        } catch (JSONException ex) {
            try {
                JSONArray jsonArray = new JSONArray(test);
                jsonArray = null;
                return true;
            } catch (JSONException ex1) {
                //System.out.println(test);
                return false;
            }
        }
    }*/
    
    public static boolean isJSONValid(String str) {
        try {
        	Gson gson = SingletonJsonParser.getInstance().getGsonInstance();
            gson.fromJson(str, Object.class);
            return true;
        } catch(com.google.gson.JsonSyntaxException ex) { 
        	LOGGER.info("String too big: " + str);
        	LOGGER.info(ex.getMessage());
            return false;
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
	protected Map<String, String> processList(String current_l_id,
			 Map<String, Collection<String>> l_idToCurrentPidCollectionMap) {
		
		Map<String, String> incomingModelCodeMap = new HashMap<String, String>();
    	String queryResultsDoc = new String();
    	Set<String> pidSet = new HashSet<String>();
    	Collection<String> searchStringsCollection = l_idToCurrentPidCollectionMap.get(current_l_id);
    	if(searchStringsCollection==null || searchStringsCollection.isEmpty()|| (searchStringsCollection.toArray())[0].toString().trim().equalsIgnoreCase(""))
    		return null;
    	
    	LOGGER.info(current_l_id + " has " + searchStringsCollection.size() + " searchTerms");
    	for(String searchString : searchStringsCollection) {
	    	String[] search = splitKeyWords(searchString);
	    	//CONSTRUCT URL - queries Solr
			//String URL1 = "http://solrx308p.stress.ch3.s.com:8180/search/select?qt=search&wt=json&q=";
	    	String URL1 = "http://solrx-prod.prod.ch4.s.com:80/search/select?qt=search&wt=json&clientID=sywAnalytics&q=";
			String URL2 = "&start=0&rows=10&fq=catalogs:%28%2212605%22%29&sort=instock%20desc,score%20desc,revenue%20desc&sortPrefix=L6;S4;10153&globalPrefix=L6,S4,10153&spuAvailability=S4&lmpAvailability=L6&fvCutoff=22&fqx=!%28storeAttributes:%28%2210153_DEFAULT_FULFILLMENT=SPU%22%29%20AND%20storeOrigin:%28%22Kmart%22%29%29&site=prod";
			String query = new String();
			
			query = URL1;
			StringBuilder sb_query = new StringBuilder(query);
			int countKeyWords=0;
		
			//if(search != null){
			//for(String keyWord:search) {
			for(int i=0; search!=null&& i< search.length; i++){
				
				//check if the search key is null
				if(search[i] == null)
					continue;
				
				if(!search[i].equalsIgnoreCase("N/A")) {
					countKeyWords++;
					if(countKeyWords==1) {
						sb_query.append(search[i]);
					}
					else {
						sb_query.append("%20");
						sb_query.append(search[i]);
					}
				}
			}
    	
    	//}
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
    	    	
    	for(String pid: pidSet) {
    		DivLn divLnObj = pidDivLnDao.getDivLnFromPid(pid);
    		if(divLnObj != null) {
	    		String div = divLnObj.getDiv();
	    		String divLn = divLnObj.getDivLn();
	    	    getIncomingModelCodeMap(div, incomingModelCodeMap);
	    		getIncomingModelCodeMap(divLn, incomingModelCodeMap);
    		}
    		   
    	}
	    	
    	return incomingModelCodeMap;
    }
/*	@Override
	protected String[] splitRec(String webRec) {
		String split[]=StringUtils.split(webRec,",");
	    if(split !=null && split.length>0) {
	        	return split;
	    }
			else {
				return null;
			}
		}*/
	
	@Override
	protected String[] splitRec(String webRec) {
		//webRec = webRec.replaceAll("[']","");
	        String split[]=StringUtils.split(webRec,",");
	        
	        if(split !=null && split.length>0) {
	        	return split;
			}
			else {
				return null;
			}
		}
}
