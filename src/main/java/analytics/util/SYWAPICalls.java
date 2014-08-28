package analytics.util;

import org.json.simple.JSONArray;

import com.github.kevinsawicki.http.HttpRequest;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SYWAPICalls {
	private static String token="0_16735_1409000859_2_37a326d1a905aaa2087878dea271d287c4edce0c081ae09f67f28bda0fce45a0";
			//0_16735_1408231577_2_9ab2bba5fbddbab1b4919d8538f53e650b43a34f14217e4bab691a5a546bec97";
	private static String hash="48cjnd7785874a1e7ca70819cf44a7b47b05ede51a9f46f9e21de87cf3ec424a96";
	//68b76c3bdf8219469997dd04004d8dade66159370e9b7b521af4f83c85cfbe61";

	private static String baseURI = "http://sandboxplatform.shopyourway.com";
    private static Gson gson = new Gson();
    

	public static String getCatalogIds(int i) {
		String requestURL = baseURI +
				"/products/get?ids="+i + 
				//"&with categories-path" + -- Requires additional permissions. Check with GUY??
				//TODO
	            "&token=" + token +
	            "&hash=" + hash;
		System.out.println(requestURL);
		JsonElement element = gson.fromJson (HttpRequest.get(requestURL).body(), JsonElement.class); 
		if(element.getAsJsonArray().size()>0)
			return ((JsonObject)element.getAsJsonArray().get(0)).get("sourceProductId").getAsString(); //ot itemId???
		else
			return "UNKNOWN";
		/*sourceProductId":"05771769000P","numberOfBuyingOptions":0,"itemId":"05771769000"*/
	}

	public static String getLoyaltyId(String userid) {
		String requestURL = baseURI +
	            "/users/get?ids=" + userid +
	            "&token=" + token +
	            "&hash=" + hash;
		System.out.println(requestURL);
		JsonElement element = gson.fromJson (HttpRequest.get(requestURL).body(), JsonElement.class); 
		if(element.isJsonArray()==false){
			System.out.println(element + "is not well formed");
			return null;
		}
		
		JsonArray jsonObj = element.getAsJsonArray();
		JsonElement loyaltyId = null;
		if(jsonObj!=null && jsonObj.size()>0)
			loyaltyId =  ((JsonObject)jsonObj.get(0)).get("sywrMemberNumber");
		if(loyaltyId==null)
			return null;
		return loyaltyId.getAsString();
	}


}
