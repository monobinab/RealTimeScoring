package analytics.cache;

import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.RegionalFactorDao;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

public class RegionalFactorCache {
	static CacheManager cm = null ;
	static Cache cache = null;
	Map<String, Double> regionalFactorsMap = new HashMap<String, Double>();
	Set<String> modelIdsWithRegionalFactor = new HashSet<String>();
	String topologyName;

	public Set<String> getModelIdsWithRegionalFactor() {
		return modelIdsWithRegionalFactor;
	}

	public void setModelIdsWithRegionalFactor(Set<String> modelIdsWithRegionalFactor) {
		this.modelIdsWithRegionalFactor = modelIdsWithRegionalFactor;
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(RegionalFactorCache.class);

	public RegionalFactorCache(){
		cache = getCache();
	}
	
	public RegionalFactorCache(String topologyName){
		this();
	}
	public static Cache getCache(){
		if(cm == null){
			InputStream urlCache = RegionalFactorCache.class.getClassLoader().getResourceAsStream("resources/ehcache.xml");
			cm = new CacheManager(urlCache);
			LOGGER.info("cache manager is created on " +  new Date() );
		}
		else{
			cm = CacheManager.getInstance();
		}
		cache = cm.getCache("regionalFactorCache");
		return cache;
	}

	public void buildCache(){
		try{
			 RegionalFactorDao dao = new RegionalFactorDao();
	 	 	 Map<String, Double> regionalFactorMap  = dao.populateRegionalFactors();
	 	 	 //populate the list of modelIdsWithRegionalFactors
	 	 	 if(regionalFactorMap != null && !regionalFactorMap.isEmpty()){
	 	 		 for(String key:regionalFactorMap.keySet()){
	 	 			 String[] modelIdState = key.split("-");
	 	 			 String modelId = modelIdState[0];
	 	 			 modelIdsWithRegionalFactor.add(modelId);
	 	 		 }
	 	 	 cache.putIfAbsent(new Element("regionalFactorsMapCache", regionalFactorMap));
	 	 	 cache.putIfAbsent(new Element("modelIdsWithRegFactorsCache", modelIdsWithRegionalFactor));
	 	 	 LOGGER.info("cache is refreshed on " +  new Date() );
	 	 	 }
	 	 }
		catch(Exception e){
			LOGGER.error("Exception in RegionalFactorCache buildCache()", e);
		}
	}

	public void refreshCache(){
		 if(cache.get("regionalFactorsMapCache") == null || cache.get("modelIdsWithRegFactorsCache") == null){
			regionalFactorsMap.clear();
			modelIdsWithRegionalFactor.clear();
			buildCache();
		}
	}

	@SuppressWarnings("unchecked")
	public Double getRegionalFactor(String modelId, String state)	{

		String key = modelId + "-" + state;
		try{
			if(cache.get("regionalFactorsMapCache") != null){
				regionalFactorsMap = (Map<String, Double>) cache.get("regionalFactorsMapCache").getObjectValue();
				if(regionalFactorsMap != null && !regionalFactorsMap.isEmpty() && regionalFactorsMap.containsKey(key)){
					return regionalFactorsMap.get(key);
				}
			}
		}
		catch(Exception e){
			LOGGER.error("Exception in RegionalFactorCache getRegonalFactor() ", e);	
		}
			return 1.0;
	}

	@SuppressWarnings("unchecked")
	public void populateModelIdsWithRegFactors(){
		try{
			refreshCache();
			if(cache.get("modelIdsWithRegFactorsCache") != null){
				setModelIdsWithRegionalFactor((Set<String>) cache.get("modelIdsWithRegFactorsCache").getObjectValue());
			}
		}
		catch(Exception e){
			LOGGER.error("Exception in RegionalFactorCache getModelIdsWithRegFactors() ", e);
		}
	}

}