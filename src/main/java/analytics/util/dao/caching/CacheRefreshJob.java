package analytics.util.dao.caching;

import java.util.ArrayList;
import java.util.List;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

public class CacheRefreshJob implements Job{

	private static final Logger LOGGER = LoggerFactory.getLogger(CacheRefreshJob.class);
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		this.clearCaches(this.getDailyActiveCaches());
		this.clearCaches(this.getDailyActiveSubCaches());
	}
	
	private void clearCaches(List<Cache> caches){
		if(caches != null && caches.size() > 0){
        	for(Cache cache : caches){
        		CacheStatistics.getInstance().showCacheStatistics(cache);
    			cache.removeAll();
    			LOGGER.info("Cache : " + cache.getName() + " refreshed successfully !!");
        	}
        }
	}

	 private List<Cache> getDailyActiveCaches(){
	    	Cache cache = null;
			List<Cache> caches = new ArrayList<Cache>();
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODELPERCENTILECACHE);
			if(cache != null){
				caches.add(cache);
			}
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODELVARIABLESCACHE);
			if(cache != null){
				caches.add(cache);
			}
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_VARIABLESCACHE);
			if(cache != null){
				caches.add(cache);
			}
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_REGIONALFACTORCACHE);
			if(cache != null){
				caches.add(cache);
			}
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODEL_BOOST_VARIABLES_CACHE);
			if(cache != null){
				caches.add(cache);
			}
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_BOOST_CACHE);
			if(cache != null){
				caches.add(cache);
			}
			return caches;
	 }
	 
	 private List<Cache> getDailyActiveSubCaches(){
	    	Cache cache = null;
			List<Cache> caches = new ArrayList<Cache>();
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_ALL_MODEL_VARIABLES_CACHE);
			if(cache != null){
				caches.add(cache);
			}
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_ALL_VARIABLE_MODELS_CACHE);
			if(cache != null){
				caches.add(cache);
			}
			cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_ALL_VARIABLE_CACHE);
			if(cache != null){
				caches.add(cache);
			}
			return caches;
	 }
}
