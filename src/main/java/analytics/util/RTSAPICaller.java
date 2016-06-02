package analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RTSAPICaller {
	
	private static RTSAPICaller instance = null;
	private static final Logger LOGGER = LoggerFactory.getLogger(RTSAPICaller.class);
	private static String RTS_API_PRE;
	private static HttpParams httpParams;
	public static RTSAPICaller getInstance() throws ConfigurationException {
		if (instance == null) {
			synchronized (RTSAPICaller.class) {
				if (instance == null)
					instance = new RTSAPICaller();
			}
		}
		return instance;
	}

	private RTSAPICaller() throws ConfigurationException {
		PropertiesConfiguration properties = null;
		String isProd = System.getProperty(MongoNameConstants.IS_PROD);
		if(isProd!=null && "PROD".equals(isProd)){
			properties=  new PropertiesConfiguration("resources/connection_config_prod.properties");
			LOGGER.info("~~~~~~~Using production properties in DBConnection~~~~~~~~~");
		}
		
		else if(isProd!=null && "QA".equals(isProd)){
			properties=  new PropertiesConfiguration("resources/connection_config.properties");
			LOGGER.info("Using test properties");	
		}
		
		else if(isProd!=null && "LOCAL".equals(isProd)){
			properties=  new PropertiesConfiguration("resources/connection_config_local.properties");
			LOGGER.info("Using test properties");	
		}
		if(null != properties && null != properties.getString("rts_api_pre_url") )
			RTS_API_PRE = properties.getString("rts_api_pre_url");
		
		httpParams =  new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(httpParams, 500);
	    HttpConnectionParams.setSoTimeout(httpParams, 500);
	}
	
	@SuppressWarnings("deprecation")
	public String getRTSAPIResponse(String lyl_l_id,String level, String key, String format, boolean isTags, String tags ) {
		String baseURL = RTS_API_PRE+lyl_l_id+"/"+level+"?key="+key+"&format="+format;
		if(isTags)
			baseURL= baseURL.concat("&tags=").concat(tags);
		String jsonRespString = null;
		HttpGet httpget = null;
		try {
			
		    HttpClient httpclient = new DefaultHttpClient(httpParams);
		    LOGGER.info("RTS API request :" + baseURL );
		    httpget = new HttpGet(baseURL);
			LOGGER.debug("executing request " + httpget.getRequestLine());
			HttpResponse response = httpclient.execute(httpget);
			if(response != null && response.getStatusLine().getStatusCode() == 200){
				String responseString = response.getStatusLine().toString();
				LOGGER.debug("RTS API Response for request : " + baseURL + " is : " + responseString);
				InputStream instream = response.getEntity().getContent();
				jsonRespString = read(instream);
				//LOGGER.info(jsonRespString);	
			}
			else{
				LOGGER.info("PERSIST: No reponse from api");
			}
		} catch(NoHttpResponseException re) {
			LOGGER.error("NoHttpResponseException Occured " + baseURL + "\n" + re.getMessage());
			LOGGER.info("PERSIST: NoHttpResponseException in RTSAPICaller"  );
		}
		catch(SocketTimeoutException se){
			LOGGER.error("SocketTimeoutException Occured " + baseURL + "\n" + se.getMessage());
			LOGGER.info("PERSIST: SocketTimeoutException in RTSAPICaller" );
		}
		catch (IOException e3) {
			LOGGER.error("IO Exception Occured " + baseURL + "\n" + e3.getMessage());
			LOGGER.info("PERSIST: IOException in RTSAPICaller" );
		} catch (Exception e5) {
			LOGGER.error("Error occured while calling the web service " + e5.getMessage());
			LOGGER.info("PERSIST: Exception in RTSAPICaller" );
		}
		finally{
			if(null != httpget){
				httpget.releaseConnection();
			}
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
	
	/*public static void main(String[] args) throws ConfigurationException {
		System.setProperty(MongoNameConstants.IS_PROD, "PROD");
		RTSAPICaller caller = RTSAPICaller.getInstance();
		String resp = caller.getRTSAPIResponse("7081010000010088", "20", "rtsTeam", "sears", false, "");
		System.out.println(resp);
	}*/

}
