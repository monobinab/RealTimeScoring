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

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.JsonUtils;

public class StrategyBoostProductTotalCount implements Strategy {
	protected final int daysToExpiration = 7;

	@Override
	public Change execute(RealTimeScoringContext context) {
		
    	Map<String, List<String>> dateValuesMap = JsonUtils.restoreDateTraitsMapFromJson((String) context.getValue());
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	int totalPidCount = 0;
    	
    	if(dateValuesMap != null && dateValuesMap.containsKey("current")) {
	    	for(String v: dateValuesMap.get("current")) {
	    		totalPidCount++;
	    	}
	    	dateValuesMap.remove("current");
	    	if(!dateValuesMap.isEmpty()) {
	    		for(String key: dateValuesMap.keySet()) {
	    			try {
						if(!new Date().after(new LocalDate(simpleDateFormat.parse(key)).plusDays(this.daysToExpiration).toDateMidnight().toDate()))
						for(String v: dateValuesMap.get(key)) {
							totalPidCount+=Integer.valueOf(v);
						}
					} catch (NumberFormatException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
	    		}
	    	}
			
			return new Change((Object) totalPidCount, calculateExpirationDate());
    	}
    	return new Change();
	}
    
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}

}
