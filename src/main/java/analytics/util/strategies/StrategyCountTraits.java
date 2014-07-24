package analytics.util.strategies;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.LocalDate;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import analytics.util.Change;
import analytics.util.RealTimeScoringContext;


public class StrategyCountTraits implements Strategy {
	private int daysToExpiration = 1;
	

    @Override
    public Change execute(RealTimeScoringContext context) {
		
    	Map<String,String> traitDateMap = restoreTraitsListFromJson((String) context.getValue());
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	
    	//FOR EACH TRAIT FOUND FROM AAM DATA FIND THE VARIABLES THAT ARE IMPACTED
    	for(String trait: traitDateMap.keySet()) {
    		if(simpleDateFormat.parse(traitDateMap.))
    		
    		
    		
    		
    			Set<String> traitCollection = traitVariablesMap.get(trait);
    			for(String variable:varCollection) {
	    			if(variableValueMap.containsKey(variable)) {
	    				int value = 1 + Integer.valueOf(variableValueMap.get(variable));
	    				variableValueMap.remove(variable);
	    				variableValueMap.put(variable, String.valueOf(value));
	    			}
	    			else {
	    				variableValueMap.put(variable, "1");
	    				//System.out.println(" *** FOUND VARIABLE: " + variable);
	    			}
	    			
	    			if(memberTraitsMap.containsKey(this.current_l_id)) {
		    			if(memberTraitsMap.get(this.current_l_id).containsKey(trait)) {
		    				memberTraitsMap.get(this.current_l_id).remove(trait);
		    			}
	    				memberTraitsMap.get(this.current_l_id).put(trait,simpleDateFormat.format(new Date()));
	    			}
	    			else {
	    				// IF MEMBER NOT FOUND IN QUERY, ADD MEMBER TO TRAIT MAP - NO HISTORY
	    				memberTraitsMap.put(this.current_l_id, new HashMap<String, String>());
	    				memberTraitsMap.get(this.current_l_id).put(trait,simpleDateFormat.format(new Date()));
	    			}
    			}
    	}
    	
    	//FOR EACH VARIABLE FOUND FIND ALL TRAITS IN MEMBER TO TRAITS MAP ASSOCIATED WITH THOSE VARIABLES, COUNT TRAITS AND RETURN
    	if(variableValueMap != null && !variableValueMap.isEmpty()) {
	    	System.out.println(" *** VARIABLE-VALUE MAP: " + variableValueMap);
    		int uniqueTraitCount = 0;
    		Map<String,String> variableUniqueCountMap = new HashMap<String,String>();
    		for(String variable:variableValueMap.keySet()) {
    			for(String trait: variableTraitsMap.get(variable)) {
    				if(memberTraitsMap.get(this.current_l_id).containsKey(trait)) {
    					boolean checkDate = false;
    					try {
    						//TODO change to current date after testing is completed
    						checkDate = simpleDateFormat.parse(memberTraitsMap.get(this.current_l_id).get(trait)).after(simpleDateFormat.parse("2014-07-14") /*format(new Date().getTime() + (-7 * 1000 * 60 * 60 * 24)*/);
						} catch (ParseException e) {
							e.printStackTrace();
						}
    					if(checkDate) {
    						uniqueTraitCount++;
    					}
    				}
    			}
    			if(uniqueTraitCount>0) {
    				variableUniqueCountMap.put(variable, String.valueOf(uniqueTraitCount));
    			}
    		}
    		if(variableUniqueCountMap!=null && !variableUniqueCountMap.isEmpty()) {
        		for(String t: variableUniqueCountMap.keySet()) {
        			System.out.println(" *** UNIQUE TRAITS COUNT FOR VARIABLE [" + t + "]: " + variableUniqueCountMap.get(t));
        		}
    			return variableUniqueCountMap;
    		}
    	}
    	else {
    		System.out.println(" *** NO TRAITS FOUND TO RESCORE");
    	}
    	return null;

    	
    	
    	
    	
    	
    	
    	
    	return new Change(new Integer(context.getPreviousValue().toString()) + context.getValue(), calculateExpirationDate());
    }

	public static Map<String, String> restoreTraitsListFromJson(String json)
    {
		Map<String, String> varList = new HashMap<String, String>();
        Type varListType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;}.getType();

        varList = new Gson().fromJson(json, varListType);
//        System.out.println(" JSON string: " + json);
//        System.out.println(" Map: " + varList);
        return varList;
    }

    private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}

}
