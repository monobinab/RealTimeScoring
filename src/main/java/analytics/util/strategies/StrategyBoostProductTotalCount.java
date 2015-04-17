package analytics.util.strategies;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.VariableDao;
import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.JsonUtils;

public class StrategyBoostProductTotalCount implements Strategy {
	protected final int daysToExpiration = 7;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StrategyBoostProductTotalCount.class);
	@Override
	public Change execute(RealTimeScoringContext context) {
		
    /*	Map<String, List<String>> dateValuesMap = JsonUtils.restoreDateTraitsMapFromJson((String) context.getValue());
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	int totalPidCount = 0;
    	
    	if(dateValuesMap != null && dateValuesMap.containsKey("current")) {
    		totalPidCount = dateValuesMap.get("current").size();
	    	dateValuesMap.remove("current");
	    	if(!dateValuesMap.isEmpty()) {
	    		for(String key: dateValuesMap.keySet()) {
	    			try {
						if(!new Date().after(new LocalDate(simpleDateFormat.parse(key)).plusDays(this.daysToExpiration).toDateMidnight().toDate()))
						for(String v: dateValuesMap.get(key)) {
							totalPidCount+=Integer.valueOf(v);
						}
					} catch (NumberFormatException e) {
						LOGGER.warn("Unable to parse date",e);
					} catch (ParseException e) {
						LOGGER.warn("Unable to parse date",e);
					}
	    		}
	    	}
			
			return new Change((Object) totalPidCount, calculateExpirationDate());
    	}*/
    	return new Change();
	}
    
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}

}
