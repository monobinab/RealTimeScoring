package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyPurchaseOccasions implements Strategy {
	private int daysToExpiration = 365;
	private int expiredDate = -1;

	@Override
	public Change execute(RealTimeScoringContext context) {
		if(context.getValue().equals("0")){//TODO: Change later
			return new Change(context.getValue(), expirationDate());
		}
		else{
			return new Change(context.getValue(), calculateExpirationDate());
		}
	}
	
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
	
	private Date expirationDate() {
		return new LocalDate(new Date()).plusDays(this.expiredDate).toDateMidnight().toDate();
	}

}
