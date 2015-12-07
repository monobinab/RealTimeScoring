package analytics.util.dao.caching;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class RTSQuartzScheduler {

	public void startCacheRefreshTask() throws SchedulerException{
		
		JobDetail job = JobBuilder.newJob(CacheRefreshJob.class).withIdentity("cacheRefreshJob", "group1").build();
		if(job != null){
			/**
			Trigger trigger = TriggerBuilder.newTrigger().
					withIdentity("cachRefreshTrigger", "group1").
					withSchedule(CronScheduleBuilder.cronSchedule("0/60 * * * * ?")).build();*/
			
			Trigger trigger = TriggerBuilder.newTrigger().
					withIdentity("cachRefreshTrigger", "group1").
					withSchedule(CronScheduleBuilder.cronSchedule("0 0 02 * * ?")).build();
			//schedule it
	    	Scheduler scheduler = new StdSchedulerFactory().getScheduler();
	    	scheduler.start();
	    	scheduler.scheduleJob(job, trigger);
		}
	}
}
