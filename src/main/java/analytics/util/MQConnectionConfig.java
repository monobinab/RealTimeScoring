package analytics.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQConnectionConfig {
	
	static final Logger logger = LoggerFactory.getLogger(MQConnectionConfig.class);
	
	public WebsphereMQCredential getWebsphereMQCredential() throws ConfigurationException{
	
    	//Configure logger
        BasicConfigurator.configure();
        
		PropertiesConfiguration properties = new PropertiesConfiguration("resources/Websphere_MQ_config.properties");
		
		WebsphereMQCredential websphereMQCredential = new WebsphereMQCredential();
		
		websphereMQCredential.setHostOneName(properties.getString("hostOne.name"));
		
		websphereMQCredential.setHostTwoName(properties.getString("hostTwo.name"));
		
		websphereMQCredential.setHostRtsThreeName(properties.getString("hostRtsThree.name"));
		
		websphereMQCredential.setHostRtsFourName(properties.getString("hostRtsFour.name"));
		
		websphereMQCredential.setPort(Integer.valueOf(properties.getString("port.no")));
		
		websphereMQCredential.setQueueOneManager(properties.getString("queueOne.manager"));
		
		websphereMQCredential.setQueueTwoManager(properties.getString("queueTwo.manager"));
		
		websphereMQCredential.setQueueRtsThreeManager(properties.getString("queueRtsThree.manager"));
		
		websphereMQCredential.setQueueRtsFourManager(properties.getString("queueRtsFour.manager"));
		
		websphereMQCredential.setQueueChannel(properties.getString("queue.channel"));
		
		websphereMQCredential.setQueueName(properties.getString("queue.name"));
		
		websphereMQCredential.setQueueRts2Channel(properties.getString("queueRts2.channel"));
		
		websphereMQCredential.setQueueRts2Name(properties.getString("queueRts2.name"));
		
		
		logger.debug("websphereMQCredential configured" + websphereMQCredential.getHostOneName()+ "," + websphereMQCredential.getHostTwoName()+ ":" + websphereMQCredential.getPort());
		
		return websphereMQCredential;
	}
	
	
	

}
