package analytics.util;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.apache.commons.codec.binary.Hex;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.main;

import com.github.kevinsawicki.http.HttpRequest;
import com.google.common.primitives.Bytes;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.disthub2.impl.matching.selector.ParseException;

public class SywApiCalls {
	/*
	 * 
	 * Sandbox details
	 * appSecret:  "7d7bdd89350c4ceda2d3ca2d0884b2a7"
	 * appID: 16735
	 * userID: 5643226
	 * URI: sandboxplatform.shopyourway.com
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SywApiCalls.class);
	private static final String APPSECRET = "70370867e53649b994b5f0175c107189";
	private static final String BASEURI = "http://platform.shopyourway.com";
	private static final String BASEURI_HTTPS = "https://platform.shopyourway.com";
	private static final Long APPID = (long) 	11875 ;
	private static final Long USERID = (long)6875997;
	
	public static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
	public static final String TOKEN_REQUEST_PATTERN = "/auth/get-token?userId=%d&appId=%d&signature=%s&timestamp=%s";
	
	private String token;
	private String hash;
	public static void main(String[] args) throws Exception {
		SywApiCalls sywApiCalls = new SywApiCalls();
		sywApiCalls.getAuthState();
		
	}
	/**
	 * Set the token, hash
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 * @throws Exception
	 */
	public SywApiCalls()  {
		try {
			token = getOfflineToken();
			hash = getHash(token);
		} catch (NoSuchAlgorithmException e) {
			LOGGER.warn("Unable to get SYW offline token and hash", e);
		} catch (UnsupportedEncodingException e) {
			LOGGER.warn("Unable to get SYW offline token and hash", e);
		}
	}
	
	/**
	 * 
	 * @param userId
	 * @param appId
	 * @param timestamp
	 * @param appSecret
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	private static String getSignature(long userId, long appId, long timestamp, String appSecret) throws NoSuchAlgorithmException, UnsupportedEncodingException {
	    StringBuilder seed = new StringBuilder();
	    MessageDigest md = MessageDigest.getInstance("SHA-256");
	    seed.append(userId).append(appId).append(timestamp).append(appSecret);
	    byte[] hash = md.digest(seed.toString().getBytes("UTF-8"));
	    md.reset();
	    return String.format("%0"+(hash.length*2)+"x", new BigInteger(1,hash));
	}
	
	/**
	 * Get Offline token
	 * @return token
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 * @throws Exception
	 */
	public String getOfflineToken() throws NoSuchAlgorithmException, UnsupportedEncodingException {
	    SimpleDateFormat formatter = new SimpleDateFormat(DATE_PATTERN);
	    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
	 
	    Date now = new Date();
	    String dateString = formatter.format(now);
	    long timestamp = (now.getTime() / 1000);
	 
	    String signature = getSignature(USERID, APPID, timestamp, APPSECRET);
	    //concat your URL - starting with your platform URL, and afterwards The request pattern for getting an offline token.
	    String requestURL = String.format(BASEURI_HTTPS + TOKEN_REQUEST_PATTERN,USERID,APPID,signature,dateString);
	    //Use your function of getting data from API's using URL.
	    String responseBody = HttpRequest.get(requestURL).body();
	    Gson gson = new Gson();
	    return gson.fromJson(responseBody, String.class);
	}
	
	/**
	 * Get the app offline hash
	 * @param sessionToken
	 * @return hash
	 * @throws NoSuchAlgorithmException 
	 * @throws UnsupportedEncodingException 
	 * @throws Exception
	 */
	public String getHash(final String sessionToken) throws NoSuchAlgorithmException, UnsupportedEncodingException
	{ 
	    MessageDigest md = MessageDigest.getInstance("SHA-256"); 
	    md.reset(); 
	    ArrayList<Byte> temp = new ArrayList<Byte>(); 
	    temp.addAll(Bytes.asList(sessionToken.getBytes("UTF-8"))); 
	    temp.addAll(Bytes.asList(APPSECRET.getBytes("UTF-8"))); 
	    return Hex.encodeHexString(md.digest(Bytes.toArray(temp))); 
	}    

	/**
	 * Make a SYW API call to request URL and retry if token is not valid
	 * @param requestURL
	 * @return
	 * @throws Exception
	 */
	private JsonElement makeGetRequestToSywAPI(String requestURL){
		Gson gson = new Gson();
		JsonElement element = gson.fromJson (HttpRequest.get(requestURL + "&token=" + token + "&hash=" + hash).body(), JsonElement.class); 
		if(element.isJsonArray()==false){
			LOGGER.info("Token expired. Re-fetching");
			//Until auth/state call is fixed, we can only retry to find if token and hash are valid
			//If it fails after 1 retry, we return null
			try {
				token = getOfflineToken();
				hash = getHash(token);
			} catch (NoSuchAlgorithmException e) {
				LOGGER.warn(e.getMessage(), e);
			} catch (UnsupportedEncodingException e) {
				LOGGER.warn(e.getMessage(), e);
			}
			//Use the new value of token and hash 
			element = gson.fromJson (HttpRequest.get(requestURL + "&token=" + token + "&hash=" + hash).body(), JsonElement.class); 
		}
		if(element.isJsonArray()==false)//if it is not well formed even after a retry
			return null;
		return element;
	}
	
	/**
	 * Get a PID - Sears/Kmart from SYW Product ID 
	 * @param Shopyourway product Id
	 * @return
	 */
	public String getCatalogId(int sywId) {
		String requestURL = BASEURI +
				"/products/get?ids="+sywId ;
		//"&with categories-path" if we want to use it? It needs additional permissions
		JsonElement element = makeGetRequestToSywAPI(requestURL);
		if(element==null)
			return null;
		JsonArray jsonObj = element.getAsJsonArray();
		JsonElement pid = null;
		if(jsonObj!=null && jsonObj.size()>0){
			pid = ((JsonObject)element.getAsJsonArray().get(0)).get("sourceProductId"); //or itemId???
		}
		if(pid==null)
			return null;
		return pid.getAsString();
		/*sourceProductId":"05771769000P","numberOfBuyingOptions":0,"itemId":"05771769000"*/
	}
	
	public String getCatalogType(int i){
		String requestURL = BASEURI + "/catalogs/get?ids="+i;
		JsonElement element = makeGetRequestToSywAPI(requestURL);
		if(element==null)
			return null;
		JsonArray jsonObj = element.getAsJsonArray();
		JsonElement type = null;
		if(jsonObj!=null && jsonObj.size()>0){
			type = ((JsonObject)element.getAsJsonArray().get(0)).get("type"); 
		}
		if(type==null)
			return null;
		return type.getAsString();
	}

	/**
	 * Get user loyalty_id from a SYW id
	 * @param userid
	 * @return
	 * @throws Exception
	 */
	public String getLoyaltyId(String userid) {
		String requestURL = BASEURI +
	            "/users/get?ids=" + userid ;
		JsonElement element = makeGetRequestToSywAPI(requestURL); 
		if(element==null)
			return null;
		JsonArray jsonObj = element.getAsJsonArray();
		JsonElement loyaltyId = null;
		if(jsonObj!=null && jsonObj.size()>0)
			loyaltyId =  ((JsonObject)jsonObj.get(0)).get("sywrMemberNumber");
		if(loyaltyId==null)
			return null;
		return loyaltyId.getAsString();
	}
	
	/**
	 * This method does not work currently. follow up with SYW to see why
	 */
	public void getAuthState(){
		String requestURL = String.format(BASEURI + "/auth/state" + "&token=" + token + "&hash=" + hash);
		Gson gson = new Gson();
		JsonElement element = gson.fromJson (HttpRequest.get(requestURL).body(), JsonElement.class);
		System.out.println(element);
	}
	
}
