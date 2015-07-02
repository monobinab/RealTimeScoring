package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.strategies.Strategy;

public class StrategyDCStrengthSum implements Strategy {

private int daysToExpiration = 30;
private double upperCapValue = 10000.0;
private double lowerCapValue = -10000.0;

@Override
    public Change execute(RealTimeScoringContext context) {
		if(context.getPreviousValue() == null || !(context.getPreviousValue() instanceof Double)) {
    		return new Change((Object) (Double.valueOf(context.getValue().toString())), calculateExpirationDate());
    	}
    	else{
    		Double changeValue = Double.valueOf(context.getPreviousValue().toString()) + Double.valueOf(context.getValue().toString());
    		if(changeValue > upperCapValue){
    			changeValue = upperCapValue;
    		}
    		else if(changeValue < lowerCapValue){
    			changeValue = lowerCapValue;
    		}
    		return new Change((Object) (changeValue), calculateExpirationDate());
    	}
    }
	
    private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
}