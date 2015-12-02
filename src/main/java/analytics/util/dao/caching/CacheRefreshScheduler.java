package analytics.util.dao.caching;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheRefreshScheduler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CacheRefreshScheduler.class);
	private static CacheRefreshScheduler cacheRefreshScheduler;
	private boolean isScheduled = Boolean.FALSE;
	
	public static CacheRefreshScheduler getInstance(){
        if(cacheRefreshScheduler == null){
        	cacheRefreshScheduler = new CacheRefreshScheduler();
        }
        return cacheRefreshScheduler;
	}
	
	public void startScheduler(){
		if(isScheduled != Boolean.TRUE){
			//LOGGER.info("QUARTZ Cache Refresh Scheduler Started Sucessfully !!");
			System.out.println("QUARTZ Cache Refresh Scheduler Started Sucessfully !!");
			isScheduled = Boolean.TRUE;
			RTSQuartzScheduler rtsQuartzScheduler = new RTSQuartzScheduler();
			try {
				rtsQuartzScheduler.startCacheRefreshTask();
			} catch (SchedulerException e) {
				LOGGER.error("Error in starting cache refresh scheduler : " + e.getMessage());
			}
		}
	}
}
