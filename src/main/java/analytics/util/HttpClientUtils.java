package analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientUtils {
	
	private static Logger LOGGER = LoggerFactory.getLogger(HttpClientUtils.class);
	/**
	 * 
	 * @param baseURL
	 * @return JSON Object with the response
	 */
	public static JSONObject httpGetCall(String baseURL){
		JSONObject result;
		HttpClient httpclient = new DefaultHttpClient();
		String jsonRespString = null;
		
		try{
			HttpGet httpget = new HttpGet(baseURL);
			httpget.setHeader("Accept", "application/json");
			HttpResponse response = httpclient.execute(httpget);
			InputStream instream = response.getEntity().getContent();
			jsonRespString = read(instream);

			JSONObject obj = new JSONObject(jsonRespString);
			
			result = obj;
		}
		catch (JSONException e2) {
			e2.printStackTrace();
			return null;
		} catch (IOException e3) {
			e3.printStackTrace();
			return null;
		} catch (Exception e5) {
			e5.printStackTrace();
			return null;
		}
		return result;
	}
	
	/**
	 * 
	 * @param in
	 * @return String being read from the stream
	 * @throws IOException
	 */
	private static String read(InputStream in) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader r = new BufferedReader(new InputStreamReader(in), 1000);
		for (String line = r.readLine(); line != null; line = r.readLine())
			sb.append(line);
		in.close();
		return sb.toString();
	}
	
	/**
	 * 
	 * @param baseURL
	 * @return
	 */
	public static JSONObject httpPostCall(String baseURL){
		JSONObject result;
		HttpClient httpclient = new DefaultHttpClient();
		String jsonRespString = null;
		
		try{
			HttpPost httpPost = new HttpPost(baseURL);
			httpPost.setHeader("Accept", "application/json");
			HttpResponse response = httpclient.execute(httpPost);
			InputStream instream = response.getEntity().getContent();
			jsonRespString = read(instream);

			JSONObject obj = new JSONObject(jsonRespString);
			
			result = obj;
		}
		catch (JSONException e2) {
			e2.printStackTrace();
			return null;
		} catch (IOException e3) {
			e3.printStackTrace();
			return null;
		} catch (Exception e5) {
			e5.printStackTrace();
			return null;
		}
		return result;
	}
	
	/**
	 *  Get a HttpURLConnection with Basic Authentication.
	 * @param baseURL
	 * @param isRequestPropertyXML
	 * @return HttpURLConnection
	 */
	public static HttpURLConnection getConnectionWithBasicAuthentication(String baseURL, String requestProperty, String requestMethodType,
																		String userName, String password) {
		LOGGER.info("Entering the createConnection method");
		HttpURLConnection connection = null;
		try {
			
			URL url = new URL(baseURL);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod(requestMethodType.toUpperCase());
			byte[] encodingBytes = Base64.encodeBase64((userName + ":" + password).getBytes());
			//System.out.println("encodedBytes " + new String(encodingBytes));

			connection.setRequestProperty("Authorization","Basic " + new String(encodingBytes));
			connection.setRequestProperty("Content-Type",requestProperty);
			LOGGER.info("After setting the content type");
			
			connection.setDoOutput(true);
			connection.setAllowUserInteraction(true);
			LOGGER.info("After setting Output");
			
		} catch (IOException ioex) {
			ioex.printStackTrace();
			LOGGER.error("Exception occured in createConnection ", ioex);
			LOGGER.error("Connection failed");
		}
		LOGGER.info("Exiting createConnection .." + connection);
		return connection;
	}

}
