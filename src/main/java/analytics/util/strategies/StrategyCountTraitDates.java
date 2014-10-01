package analytics.util.strategies;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class StrategyCountTraitDates implements Strategy {
	private int daysToExpiration = 1;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StrategyCountTraitDates.class);

    @Override
    public Change execute(RealTimeScoringContext context) {
		
    	Map<String,List<String>> dateTraitsMap = JsonUtils.restoreDateTraitsMapFromJson((String) context.getValue());
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	int dateCount = 0;
    			
    	//FOR EACH TRAIT FOUND FROM AAM DATA FIND THE VARIABLES THAT ARE IMPACTED
    	for(String date: dateTraitsMap.keySet()) {
    		try {
				if(simpleDateFormat.parse(date).after(new Date(new Date().getTime() + (-7 * 1000 * 60 * 60 * 24)))) {
					if(!dateTraitsMap.get(date).isEmpty()) {
						dateCount++;
					}
				}
			} catch (ParseException e) {
				LOGGER.warn("Unable to parse date",e);
			}
    	}
    	
    	return new Change(dateCount, calculateExpirationDate());
    }

    private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}


}
