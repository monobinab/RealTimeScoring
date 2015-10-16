package analytics.util;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import analytics.util.dao.ModelPercentileDao;

public class ScoringUtils {
	private Map<Integer,TreeMap<Integer,Double>> modelScorePercentileMap;
	private ModelPercentileDao modelPercentileDao;

	public ScoringUtils(){
		modelPercentileDao = new ModelPercentileDao();
		modelScorePercentileMap = getModelScorePercentileMap();
	}

	private HashMap<Integer,TreeMap<Integer,Double>> getModelScorePercentileMap(){
		HashMap<Integer,TreeMap<Integer,Double>> modelScorePercentileMap = modelPercentileDao.getModelScorePercentilesMap();
		return modelScorePercentileMap;
	}
	
	public int getPercentileForScore(double score, int modelId){
		TreeMap<Integer,Double> percMap = modelScorePercentileMap.get(modelId);
		int p = 0;
		if (percMap != null && percMap.size() > 0) {
			for (int i = percMap.size() - 1; i >= 1; i--) {
				if (score > percMap.get(i)) {
					p = i + 1;
					break;
				}
			}
			if (p == 0) {
				p = 1;
			}
		}
		return p;
	}
}
