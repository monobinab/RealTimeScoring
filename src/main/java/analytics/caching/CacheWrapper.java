package analytics.caching;

import org.apache.commons.lang3.StringUtils;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

public class CacheWrapper {

	private static CacheWrapper cacheWrapper;
	
	private CacheWrapper(){}
	
	public static CacheWrapper getInstance(){
        if(cacheWrapper == null){
        	cacheWrapper = new CacheWrapper();
        }
        return cacheWrapper;
	}
	
	public Element isCacheKeyExist(Cache cache, String key){
		Element element = null;
		if(cache != null && StringUtils.isNotEmpty(key)){
			element = cache.get(key);
		}
		return element;
	}
}
