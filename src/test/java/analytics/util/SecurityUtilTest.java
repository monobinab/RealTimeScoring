package analytics.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Test;

public class SecurityUtilTest {
	@Test
	public void testSimulataneousCallsAreSuccessful() throws InterruptedException {
		HashThread[] threads = new HashThread[10];
		for(int i=0;i<10;i++)
		{
			threads[i] = new HashThread();
			threads[i].start();
		}
		for(int i=0;i<10;i++)
		{
			threads[i].join();
			Assert.assertEquals(10, (threads[i]).getCounter());
		}
	}
	
	private class HashThread extends Thread {
		Map<String, String> loyaltyIdToLId;
		int counter;
		public HashThread() {
			this.counter = 0;
			loyaltyIdToLId = new HashMap<String, String>();
				loyaltyIdToLId.put("7081187606915702", "PX/88Std5IM7m55AZicxDwDMMaQ=");
				loyaltyIdToLId.put("7081187568761581", "STDxQ19U87JhDyJ8a7Sb81DOZCk=");
				loyaltyIdToLId.put("7081197557918522", "wA3dlVQRkn4Frco5NNqjxDSozB8=");
				loyaltyIdToLId.put("7081197553832875", "Ut5QJQffkB4q5XNzkkdigkf4b0Q=");
				loyaltyIdToLId.put("7081197561874059", "ppbs1J+e0Xxs8LJsQArxU5hyn/w=");
				loyaltyIdToLId.put("7081197559131454", "LC2Hz8/gzWVDwAqm+GvYet+LsIk=");
				loyaltyIdToLId.put("7081197559714788", "f8WpbNJOG/HsuPqSv1Nn16VQ1no=");
				loyaltyIdToLId.put("7081197561509408", "ZZ/4TNCRJMbo9h0tCK7X9l5sUlU=");
				loyaltyIdToLId.put("7081197559373296", "f08zyQa1/+6rIJwwa6cTDIWzJec=");
				loyaltyIdToLId.put("7081197555853507", "9SE6SrtUfJyutt+nPD4GeMif/eU=");
		}

	    public void run() {
	    	for (Entry<String, String> loyalId : loyaltyIdToLId.entrySet())
	    	{
	    		if(loyalId.getValue().equals(SecurityUtils.hashLoyaltyId(loyalId.getKey()))){
	    			counter++;
	    		}
	    	}
	    }
	    
	    public int getCounter(){
	    	return counter;
	    }
	}
}
