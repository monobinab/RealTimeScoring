package analytics.util.dao.caching;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

public class CacheRefreshScheduler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CacheRefreshScheduler.class);
	private static CacheRefreshScheduler cacheRefreshScheduler;
	private boolean isScheduled = Boolean.FALSE;
	
	public static CacheRefreshScheduler getInstance(){
		System.out.println("CacheRefreshScheduler - getInstance called");
        if(cacheRefreshScheduler == null){
        	cacheRefreshScheduler = new CacheRefreshScheduler();
        }
        return cacheRefreshScheduler;
	}
	
	public void startScheduler(){
		if(isScheduled != Boolean.TRUE){
			isScheduled = Boolean.TRUE;
			final List<Cache> caches = this.getActiveCaches();
			Timer t = new Timer(true);
		    Integer interval = 24 * 60 * 60 * 1000; //24 hours
		    Calendar c = Calendar.getInstance();
		    c.set(Calendar.HOUR, 0);
		    c.set(Calendar.MINUTE, 0);
		    c.set(Calendar.SECOND, 0);
		    t.scheduleAtFixedRate( new TimerTask() {
	            public void run() {
	            	if(caches != null && caches.size() > 0){
	            		for(Cache cache : caches){         
	            			cache.removeAll();
	            		}
	            	}
	            }
	        }, c.getTime(), interval);
		}
	}
	
	private List<Cache> getActiveCaches(){
		List<Cache> caches = new ArrayList<Cache>();
		CacheConstant cacheConstant = new CacheConstant();
		try {
		Field[] fields = cacheConstant.getClass().getDeclaredFields();
		for (Field field : fields ) {
			  if(StringUtils.isNotEmpty(field.getName()) && (field.getName().contains("RTS_CACHE_") && !field.getName().contains("_KEY"))){
				  field.setAccessible(true);
				  Cache cache = CacheManager.getInstance().getCache((String)field.get(cacheConstant));
					if(cache != null){
						caches.add(cache);
					}
			  	}
			}
		} catch (IllegalArgumentException e) {
			LOGGER.error("Error reading cache " + e.getMessage());
		} catch (IllegalAccessException e) {
			LOGGER.error("Error reading cache " + e.getMessage());
		}
		return caches;
	}
}
