package analytics.util.objects;

import java.util.HashMap;
import java.util.Map;

import analytics.util.strategies.Strategy;
import analytics.util.strategies.StrategyBoostProductTotalCount;
import analytics.util.strategies.StrategyCountTraitDates;
import analytics.util.strategies.StrategyCountTraits;
import analytics.util.strategies.StrategyCountTransactions;
import analytics.util.strategies.StrategyDaysSinceLast;
import analytics.util.strategies.StrategySumSales;
import analytics.util.strategies.StrategyTurnOffFlag;
import analytics.util.strategies.StrategyTurnOnFlag;

public class StrategyMapper {
	
	private static StrategyMapper strategyMapperInstance;
	private Map<String, Strategy> strategyMap;
	
	public StrategyMapper() {
		strategyMap = new HashMap<String, Strategy>();
   		populateStrategyMap();
	}
	
	public static StrategyMapper getInstance() {
		if(strategyMapperInstance == null) {
			strategyMapperInstance = new StrategyMapper();
		}
		return strategyMapperInstance;
	}

	
    public Strategy getStrategy(String strategy) {

    	if(strategyMap.containsKey(strategy)) {
			return strategyMap.get(strategy);
		}
		
    	return null;
    }
    
    private void populateStrategyMap() {
		strategyMap.put("StrategyCountTraitDates", new StrategyCountTraitDates()); 
		strategyMap.put("StrategyCountTraits", new StrategyCountTraits()); 
		strategyMap.put("StrategyCountTransactions", new StrategyCountTransactions()); 
		strategyMap.put("StrategyDaysSinceLast", new StrategyDaysSinceLast()); 
		strategyMap.put("StrategySumSales", new StrategySumSales()); 
		strategyMap.put("StrategyTurnOffFlag", new StrategyTurnOffFlag()); 
		strategyMap.put("StrategyTurnOnFlag", new StrategyTurnOnFlag());
		strategyMap.put("StrategyBoostProductTotalCount", new StrategyBoostProductTotalCount());
    }
    
}
