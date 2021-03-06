package analytics.util.strategies;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import analytics.util.JsonUtils;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;


public class StrategyCountTraits implements Strategy {
	protected final int daysToExpiration = 1;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StrategyCountTraits.class);

    @Override
    public Change execute(RealTimeScoringContext context) {
		
    	Map<String, List<String>> dateTraitsMap = JsonUtils.restoreDateTraitsMapFromJson((String) context.getValue());
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	Set<String> traitSet = new HashSet<String>();
    	int traitCount = 0;
    			
    	//FOR EACH TRAIT FOUND FROM AAM DATA FIND THE VARIABLES THAT ARE IMPACTED
    	for(Map.Entry<String, List<String>> entry : dateTraitsMap.entrySet()){
    		String date = entry.getKey();
    		Collection<String> traits = entry.getValue();
    		try {
				if(simpleDateFormat.parse(date).after(new Date(new Date().getTime() + (-7 * 1000 * 60 * 60 * 24)))) {
					for(String trait: traits) {
						if(traitSet.add(trait)) {
							traitCount++;
						}
					}
				}
			} catch (ParseException e) {
				LOGGER.warn("Unable to parse date",e);
			}
    	}
    	
    	return new Change(traitCount, calculateExpirationDate());
    }

	
    private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}


	@Override
	public Change executeBlackout(RealTimeScoringContext context,
			Date transactionDate) {
		// TODO Auto-generated method stub
		return null;
	}

}
