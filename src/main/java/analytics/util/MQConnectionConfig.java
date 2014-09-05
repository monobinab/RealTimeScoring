package analytics.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.RealTimeScoringTopology;

public class MQConnectionConfig {
	
	static final Logger logger = LoggerFactory.getLogger(MQConnectionConfig.class);
	
	public WebsphereMQCredential getWebsphereMQCredential() throws ConfigurationException{
	
    	//Configure logger
        BasicConfigurator.configure();
        
		PropertiesConfiguration properties = new PropertiesConfiguration("resources/Websphere_MQ_config.properties");
		
		WebsphereMQCredential websphereMQCredential = new WebsphereMQCredential();
		
		websphereMQCredential.setHostOneName(properties.getString("hostOne.name"));
		
		websphereMQCredential.setHostTwoName(properties.getString("hostTwo.name"));
		
		websphereMQCredential.setPort(Integer.valueOf(properties.getString("port.no")));
		
		websphereMQCredential.setQueueOneManager(properties.getString("queueOne.manager"));
		
		websphereMQCredential.setQueueTwoManager(properties.getString("queueTwo.manager"));
		
		websphereMQCredential.setQueueChannel(properties.getString("queue.channel"));
		
		websphereMQCredential.setQueueName(properties.getString("queue.name"));
		
		logger.info("websphereMQCredential Port is..." +websphereMQCredential.getPort());
		
		return websphereMQCredential;
	}
	
	
	

}
