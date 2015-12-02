package analytics.util.dao.caching;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;


public class RTSTimerTask extends TimerTask {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RTSTimerTask.class);
    private final static long ONCE_PER_DAY = 1000*60*60*24;
    private final static int TWO_AM = 2;
    private final static int ZERO_MINUTES = 0;
    private final SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");  


    @Override
    public void run() {
    	Date currentDate = new Date(System.currentTimeMillis());
    	LOGGER.info("Daily Cache Refresh Job Started at : " + sdf.format(currentDate));
        List<Cache> caches = this.getDailyActiveCaches();
        if(caches != null && caches.size() > 0){
        	for(Cache cache : caches){         
    			cache.removeAll();
    			LOGGER.info("Cache : " + cache.getName() + " refreshed successfully !!");
        	}
        }
   }
    
    private Date getTomorrowMorning2AM(){
        Date date2am = new java.util.Date(); 
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(date2am);
        calendar.set(Calendar.HOUR_OF_DAY, TWO_AM);
        calendar.set(Calendar.MINUTE, ZERO_MINUTES);
        return calendar.getTime();
    }
    
    public void startTask(){
    	RTSTimerTask task = new RTSTimerTask();
        Timer timer = new Timer();  
        //timer.schedule(task, getTomorrowMorning2AM(), ONCE_PER_DAY);// Once Per Day - 1000*60*60*24 (24 hrs)
        timer.schedule (task,0,1000*60*1);
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
		return caches;
	}
}
