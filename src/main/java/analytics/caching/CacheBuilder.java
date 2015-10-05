package analytics.caching;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

public class CacheBuilder {

	private static CacheBuilder cacheBuilder;
	
	private static CacheManager cacheManager;
	
	private CacheBuilder(){}
	
	public static CacheBuilder getInstance(){
        if(cacheBuilder == null){
        	cacheManager = CacheManager.newInstance();
        	cacheBuilder = new CacheBuilder();
        }
        return cacheBuilder;
	}
	
	public Cache getCache(String cacheName){
		return cacheManager.getCache(cacheName);
	}
}
