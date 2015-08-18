package analytics.util;

import java.util.ArrayList;
import java.util.HashMap;


public class Testing {

	public static void main(String[] args) {
		/*double newScore = Math.exp(36) / (1 + Math.exp(36));
		System.out.println(newScore);*/
	/*	System.setProperty("rtseprod", "LOCAL");
		ScoringSingleton sc = ScoringSingleton.getInstance();
		ArrayList<String> modelIdList = new ArrayList<String>();
		modelIdList.add("76");
		modelIdList.add("78");
		modelIdList.add("8");
		
		HashMap<String, Double> mbrChanges = sc.execute("BfdA+wNN9wbHYbJwCDAgLR+pf0s=", modelIdList, "RESCORED");*/
		
		double score =  Math.exp(5.07) / (1 + Math.exp(5.07));
		System.out.println(score);

	}

}
