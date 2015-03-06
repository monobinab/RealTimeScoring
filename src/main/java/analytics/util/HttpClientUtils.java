package analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.json.JSONObject;

public class HttpClientUtils {
	
	
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

}
