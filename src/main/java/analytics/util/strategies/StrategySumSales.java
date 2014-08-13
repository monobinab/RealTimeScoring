package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.Change;
import analytics.util.RealTimeScoringContext;

public class StrategySumSales implements Strategy {

	private int daysToExpiration = 2;
	

    @Override
    public Change execute(RealTimeScoringContext context) {
		//return new Change(new Double(context.getPreviousValue().toString()) + context.getValue(), calculateExpirationDate());
    	if(context.getPreviousValue() == null || !(context.getPreviousValue() instanceof Double)) {
    		return new Change((Object) (Double.valueOf(context.getValue().toString())), calculateExpirationDate());
    	}
		return new Change((Object) (Double.valueOf(context.getPreviousValue().toString()) + Double.valueOf(context.getValue().toString())), calculateExpirationDate());
    }
	
    private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
}
