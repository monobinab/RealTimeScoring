package analytics.util.strategies;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class StrategyCountTraitDates implements Strategy {
	private int daysToExpiration = 1;
	

    @Override
    public Change execute(RealTimeScoringContext context) {
		
    	Map<String,Collection<String>> dateTraitsMap = restoreDateTraitsMapFromJson((String) context.getValue());
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
				e.printStackTrace();
			}
    	}
    	
    	return new Change(dateCount, calculateExpirationDate());
    }

	public static Map<String, Collection<String>> restoreDateTraitsMapFromJson(String json)
    {
		Map<String, Collection<String>> dateTraitsMap = new HashMap<String, Collection<String>>();
        Type dateTraitType = new TypeToken<Map<String, Collection<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		dateTraitsMap = new Gson().fromJson(json, dateTraitType);
//        System.out.println(" JSON string: " + json);
//        System.out.println(" Map: " + dateTraitsMap);
        return dateTraitsMap;
    }

    private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}


}
