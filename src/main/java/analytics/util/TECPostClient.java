package analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.ArrayList;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TECPostClient {
	
	private static TECPostClient instance = null;
	public static final String TEC_API = "http://semantictec.com/message/produce";
	private static final Logger LOGGER = LoggerFactory.getLogger(TECPostClient.class);
	
	public static TECPostClient getInstance() {
		if (instance == null) {
			synchronized (TECPostClient.class) {
				if (instance == null)
					instance = new TECPostClient();
			}
		}
		return instance;
	}
	
	private TECPostClient() {
		
	}
	
	public static void postToTEC(String message, String l_id) {
		HttpResponse response = null;
		try {
			HttpClient httpclient = new DefaultHttpClient();			
			HttpPost httppost = new HttpPost(TEC_API);
			ArrayList<NameValuePair> postParameters;
			postParameters = new ArrayList<NameValuePair>();
			postParameters.add(new BasicNameValuePair("topic", "user.tags.change"));		
			postParameters.add(new BasicNameValuePair("message", message));
			httppost.setEntity(new UrlEncodedFormEntity(postParameters));

			response = httpclient.execute(httppost);
			if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
				LOGGER.error(" Error :: Posting to TEC was not successful. Reason - "+ response.getStatusLine().toString());			
	 
		  } catch (MalformedURLException e) {	 
			  LOGGER.error("Exception in TECPostClient class - " + e.getMessage());	 
		  } catch (IOException e) {	 
			  LOGGER.error("Exception in TECPostClient class - " + e.getMessage());	 
		  }
		  finally{
			  if( response != null)
				  try {					  
					EntityUtils.consume(response.getEntity());
				  } catch (IOException e) {
					 LOGGER.error("Exception in TECPostClient class - " + e.getMessage());
				  }
		  }
	}
	 

}
