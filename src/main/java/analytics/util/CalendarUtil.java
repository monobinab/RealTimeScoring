package analytics.util;

import java.util.Calendar;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalendarUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(CalendarUtil.class);
	
	public static Date getTodaysDate()
	{
		Calendar calendar = Calendar.getInstance();
		Date currentDate = calendar.getTime();
		return new Date(currentDate.getTime());
	}

	//returns a new date by adding 
	public static Date getNewDate(Date oldDate,Integer daysToAdd) {
		Calendar c = Calendar.getInstance();
		c.setTime(oldDate);		
		c.add(Calendar.DATE,daysToAdd);
		return new Date(c.getTimeInMillis());
	}

	public static Integer getDatesDiff(Date newDate, Date oldDate) {
		SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		
		try {
			String strStartDate = sdformat.format(newDate);
			String strEndDate = sdformat.format(oldDate);		
			DateTime dt1 = new DateTime(sdformat.parse(strStartDate));
			DateTime dt2 = new DateTime(sdformat.parse(strEndDate));
			return Days.daysBetween(dt2,dt1).getDays();
		} catch (ParseException e) {
			LOGGER.error("Exception occured in calculating the difference between dates : " + e.getMessage());
		}
		return  null;
		
	}
	/**
	 * SimpleDateFormat is not designed to be used by multiple threads at the same time
	 * Thus a new instance is needed to be created every time upon used. Better than synchronizing the object for now.
	 * @return
	 */
	public static SimpleDateFormat getDateFormat(){
		return new SimpleDateFormat("yyyy-MM-dd");
	}
}
