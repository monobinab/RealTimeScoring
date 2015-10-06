package analytics.util.dao.caching;

import java.util.ArrayList;
import java.util.List;

import net.sf.ehcache.Cache;

public class CacheBuilder {

	private static CacheBuilder cacheBuilder;
	
	private static List<Cache> caches = null;
	
	private CacheBuilder(){}
	
	public static CacheBuilder getInstance(){
        if(cacheBuilder == null){
        	cacheBuilder = new CacheBuilder();
        	caches = new ArrayList<Cache>();
        }
        return cacheBuilder;
	}
	
	public void setCaches(Cache cache){
		caches.add(cache);
	}
	
	public List<Cache> getCaches(){
		return caches;
	}
}
