package analytics.util.strategies;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.joda.time.LocalDate;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyDCFlag implements Strategy {

	private int daysToExpiration = 30;
	private int flag = 0;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StrategyBoostProductTotalCount.class);

	@Override
	public Change execute(RealTimeScoringContext context) {
		// return new Change(new Double(context.getPreviousValue().toString()) +
		// context.getValue(), calculateExpirationDate());
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Object contextPrevVal = context.getPreviousValue();
		Object contextVal = context.getValue();
		String text = (String) contextVal;
		//code might break here if text cannot be parsed to a map
		HashMap<String, Object> dateDCMap = (HashMap<String, Object>) JsonUtils.restoreDateDCMapFromJson((String) text);
		Iterator<String> keyIt = dateDCMap.keySet().iterator();
		Double totalUnexpiredStrength = 0.0;
		while (keyIt.hasNext()) {
			String date = keyIt.next();
			try {
				// if haven't expired
				if (!new Date().after(new LocalDate(simpleDateFormat.parse(date)).plusDays(this.daysToExpiration).toDateMidnight().toDate())) {
					Object value = dateDCMap.get(date);
					totalUnexpiredStrength += JsonUtils.convertToDouble(value);
				}
			} catch (ParseException e) {
				LOGGER.warn("Unable to parse date",e);
				e.printStackTrace();
			}

		}
		// get the sum from the past 30 days and compare with 0 and return flag
		if ( totalUnexpiredStrength > 0) {
			this.flag = 1;
		}

		return new Change((Object) this.flag, calculateExpirationDate());
	}

	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}

}
