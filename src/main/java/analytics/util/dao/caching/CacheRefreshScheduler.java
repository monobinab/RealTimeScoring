package analytics.util.dao.caching;

public class CacheRefreshScheduler {
	
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
			isScheduled = Boolean.TRUE;
			RTSTimerTask rtsTimerTask = new RTSTimerTask();
			rtsTimerTask.startTask();
		}
	}
}
