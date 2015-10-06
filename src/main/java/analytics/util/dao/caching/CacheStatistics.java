package analytics.util.dao.caching;

import java.util.Date;
import java.util.List;

import net.sf.ehcache.Cache;

public class CacheStatistics implements Runnable{
	
	private Thread thread;
	private String threadName;
	
	public CacheStatistics(String threadName){
		this.threadName = threadName;
	}

	public void printCacheStatistics(String cacheName){
		List<Cache> caches = CacheBuilder.getInstance().getCaches();
		if(caches != null && caches.size() > 0){
			for(Cache cache : caches){
				StringBuilder cacheStatsBuilder = new StringBuilder();
				cacheStatsBuilder.append(new Date()).append("\n")
				.append("Cache Size : " + cache.getStatistics().getSize()).append("\n")
				.append("Cache Hit Count : " + cache.getStatistics().cacheHitCount()).append("\n")
				.append("Cache Miss Count : " + cache.getStatistics().cacheMissCount()).append("\n")
				.append("Cache Hit Ratio : " + cache.getStatistics().cacheHitRatio()).append("\n")
				.append("Cache Put Count : " + cache.getStatistics().cachePutCount()).append("\n")
				.append("Cache Remove Count : " + cache.getStatistics().cacheRemoveCount()).append("\n")
				.append("Cache Heap Size : " + cache.getStatistics().getLocalHeapSize()).append("\n");
				System.out.println(cacheStatsBuilder.toString());
			}
		}
	}

	@Override
	public void run() {
		for(;;){
			try {
			this.printCacheStatistics(RTSCacheConstant.RTS_CACHE_MODELPERCENTILECACHE);
			Thread.sleep(6000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void start(){
		System.out.println("Starting " +  threadName );
	      if (thread == null){
	    	  thread = new Thread (this, threadName);
	    	  thread.start ();
	      }
	}
	
	public static void main(String args[]){
		CacheStatistics cacheStatistics = new CacheStatistics("Cache Stats Thread");
		cacheStatistics.start();
	}
}
