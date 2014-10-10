package analytics.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQConnectionConfig {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MQConnectionConfig.class);
	
	public WebsphereMQCredential getWebsphereMQCredential(String feed) throws ConfigurationException{
		PropertiesConfiguration properties=null;
		String isProd = System.getProperty(MongoNameConstants.IS_PROD);
    	//Configure logger
        BasicConfigurator.configure();
        if(feed.equals("Telluride")){
        	if("true".equals(isProd))
        		properties = new PropertiesConfiguration("resources/Telluride_MQ_Prod_config.properties");
        	else
        		properties = new PropertiesConfiguration("resources/Telluride_MQ_config.properties");
        }
        else if(feed.equals("POS")){
        	if("true".equals(isProd))
        		properties = new PropertiesConfiguration("resources/POS_MQ_config.properties");
        }
        if(properties==null){
        	LOGGER.warn("Unable to find feed or property file for given feed");
        	return null;
        }

		WebsphereMQCredential websphereMQCredential = new WebsphereMQCredential();
		
		websphereMQCredential.setHostOneName(properties.getString("hostOne.name"));
		
		websphereMQCredential.setHostTwoName(properties.getString("hostTwo.name"));
		
		websphereMQCredential.setPort(Integer.valueOf(properties.getString("port.no")));
		
		websphereMQCredential.setQueueOneManager(properties.getString("queueOne.manager"));
		
		websphereMQCredential.setQueueTwoManager(properties.getString("queueTwo.manager"));
		
		websphereMQCredential.setQueueChannel(properties.getString("queue.channel"));
		
		websphereMQCredential.setQueueName(properties.getString("queue.name"));		
		
		LOGGER.debug("websphereMQCredential configured" + websphereMQCredential.getHostOneName()+ "," + websphereMQCredential.getHostTwoName()+ ":" + websphereMQCredential.getPort());
		
		return websphereMQCredential;
	}
	
	
	

}
