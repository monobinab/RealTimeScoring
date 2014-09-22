package analytics.util.strategies;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    	List<String> valuesList = new ArrayList<String>();
    	int currentPidCount = 0;
    	
    	for(String v: dateValuesMap.get("current")) {
    		currentPidCount++;
    	}
    	if(dateValuesMap.containsKey(simpleDateFormat.format(new Date()))) {
    		for(String v: dateValuesMap.get(simpleDateFormat.format(new Date()))) {
    			currentPidCount+=Integer.valueOf(v);
    		}
    	}
		
		return new Change("1", (Object) currentPidCount, calculateExpirationDate());
	}
    
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}

}
