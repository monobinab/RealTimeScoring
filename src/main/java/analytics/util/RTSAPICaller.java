package analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RTSAPICaller {
	
	private static RTSAPICaller instance = null;
	public static final String RTS_API_PRE = "http://rtsapi301p.qa.ch3.s.com:8180/rtsapi/v1/top/categories/";
	//public static final String SCORING_API_POST = "/16?key=za4n47bd&tags=models"; 
	private static final Logger LOGGER = LoggerFactory.getLogger(RTSAPICaller.class);
	
	public static RTSAPICaller getInstance() {
		if (instance == null) {
			synchronized (RTSAPICaller.class) {
				if (instance == null)
					instance = new RTSAPICaller();
			}
		}
		return instance;
	}
	
	private RTSAPICaller() {
		
	}
	
	
	public String getRTSAPIResponse(String lyl_l_id,String level, String key, String format, boolean isTags, String tags ){
		String baseURL = RTS_API_PRE+lyl_l_id+"/"+level+"?key="+key+"&format="+format;
		
		if(isTags)
			baseURL= baseURL+"&tags="+tags;
		String jsonRespString = null;
		try {
			HttpClient httpclient = new DefaultHttpClient();
			HttpGet httpget = new HttpGet(baseURL);

			LOGGER.debug("executing request " + httpget.getRequestLine());
			HttpResponse response = httpclient.execute(httpget);
			String responseString = response.getStatusLine().toString();
			LOGGER.debug("RTS API Response : " + responseString);
			InputStream instream = response.getEntity().getContent();
			jsonRespString = read(instream);
			//LOGGER.info(jsonRespString);	

		} catch (IOException e3) {
			e3.printStackTrace();
			LOGGER.error("IO Exception Occured " + baseURL + "\n" + e3);
			return null;
		} catch (Exception e5) {
			e5.printStackTrace();
			LOGGER.error("Error occured while calling the web service " + e5);
			return null;
		}
		return jsonRespString;
		
	}
	
	/**
	 * 
	 * @param in
	 * @return String
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
