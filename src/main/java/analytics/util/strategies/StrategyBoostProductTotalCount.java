package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyBoostProductTotalCount implements Strategy{

	protected final int daysToExpiration = 7;
	private int upperCapValue = 12;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StrategyBoostProductTotalCount.class);
	
	
	@Override
	public Change execute(RealTimeScoringContext context) {
		int changeValue = Integer.valueOf(context.getValue().toString());
		if(changeValue > upperCapValue)
			changeValue = upperCapValue;
		return new Change((Object)changeValue, calculateExpirationDate());
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
