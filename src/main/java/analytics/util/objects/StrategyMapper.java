package analytics.util.objects;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.ScoringSingleton;
import analytics.util.dao.VariableDao;
import analytics.util.strategies.Strategy;

public class StrategyMapper {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ScoringSingleton.class);
	private static StrategyMapper strategyMapperInstance = null;
	private Map<String, Strategy> strategyMap;
	
	private StrategyMapper() {
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
    	VariableDao variableDao = new VariableDao();
    	Set<String> strategyList = variableDao.getStrategyList();
    	Strategy strategy;
    	
    	for(String s: strategyList) {
    		//TODO: remove this line after the strategy is added
    		//if(s.startsWith("StrategySyw"))continue;
    		String fullyQualifiedName = "analytics.util.strategies."+s;
    		Class<?> strategyClass;
			try {
				strategyClass = Class.forName(fullyQualifiedName);
				strategy = (Strategy) strategyClass.newInstance();
	    		strategyMap.put(s, strategy);
			} catch (ClassNotFoundException e) {
				LOGGER.warn("Strategy class does not exist to populate StrategyMap in StrategyMapper class: ");
				e.printStackTrace();
			} catch (InstantiationException e) {
				LOGGER.warn("Could not instantiate Strategy class for StrategyMapper: ");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				LOGGER.warn("StrategyMapper exception: ");
				e.printStackTrace();
			}
    	}
      	
//		strategyMap.put("StrategyCountTraitDates", new StrategyCountTraitDates()); 
//		strategyMap.put("StrategyCountTraits", new StrategyCountTraits()); 
//		strategyMap.put("StrategyCountTransactions", new StrategyCountTransactions()); 
//		strategyMap.put("StrategyDaysSinceLast", new StrategyDaysSinceLast()); 
//		strategyMap.put("StrategySumSales", new StrategySumSales()); 
//		strategyMap.put("StrategyTurnOffFlag", new StrategyTurnOffFlag()); 
//		strategyMap.put("StrategyTurnOnFlag", new StrategyTurnOnFlag());
//		strategyMap.put("StrategyBoostProductTotalCount", new StrategyBoostProductTotalCount());
//		strategyMap.put("StrategyDCFlag", new StrategyDCFlag());
    }
    
}
