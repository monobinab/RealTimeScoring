package analytics.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnection.class);
	public static String[] getServers(){
		Properties prop = new Properties();
		String isProd = System.getProperty(MongoNameConstants.IS_PROD);
		try {
		    //load a properties file from class path, inside static method
			if(isProd!=null && "true".equals(isProd)){
				prop.load(RedisConnection.class.getClassLoader().getResourceAsStream("resources/redis_server_prod.properties"));
				LOGGER.info("Using production properties");
			}
			else{
				prop.load(RedisConnection.class.getClassLoader().getResourceAsStream("resources/redis_server.properties"));
				LOGGER.info("Using test properties");	
			}	
			
		    List<String> servers= new ArrayList<String>();
		    int i=1;
		    while(prop.containsKey("server"+i)){
		    	servers.add(prop.getProperty("server"+i));
		    	i++;
		    }
		    return servers.toArray(new String[i-1]);
		} 
		catch (IOException ex) {
		    LOGGER.error("Unable to get server names",ex);
		    return null;
		}
	}
}
