package analytics.util.dao.caching;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.ehcache.Cache;

public class CacheStatistics{

	private static final Logger LOGGER = LoggerFactory.getLogger(CacheStatistics.class);
	private static CacheStatistics cacheStatistics;
	
	public static CacheStatistics getInstance(){
        if(cacheStatistics == null){
        	cacheStatistics = new CacheStatistics();
        }
        return cacheStatistics;
	}
	
	public void printCacheStatistics(){
		List<Cache> caches = CacheBuilder.getInstance().getCaches();
		if(caches != null && caches.size() > 0){
			for(Cache cache : caches){
				StringBuilder cacheStatsBuilder = new StringBuilder();
				cacheStatsBuilder.append(new Date()).append("\n")
				.append("Cache Name : " + cache.getName()).append("\n")
				.append("Cache Size : " + cache.getStatistics().getSize()).append("\n")
				.append("Cache Hit Count : " + cache.getStatistics().cacheHitCount()).append("\n")
				.append("Cache Miss Count : " + cache.getStatistics().cacheMissCount()).append("\n")
				.append("Cache Hit Ratio : " + cache.getStatistics().cacheHitRatio()).append("\n")
				.append("Cache Put Count : " + cache.getStatistics().cachePutCount()).append("\n")
				.append("Cache Remove Count : " + cache.getStatistics().cacheRemoveCount()).append("\n")
				.append("Cache Heap Size : " + cache.getStatistics().getLocalHeapSize()).append("\n");
				LOGGER.info(cacheStatsBuilder.toString());
				System.out.println(cacheStatsBuilder.toString());
			}
		}
	}
}
