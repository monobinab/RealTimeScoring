package analytics.util;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

public class KafkaUtil {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(KafkaUtil.class);

	private static final String PRODUCTION = "PROD";
	private static final String QA = "QA";
	private static final String LOCAL = "LOCAL";
	private static final String RESOURCE = "resources";
	// Kafka related constants
	private static final String ZOOKEEPER = "zookeeper";
	private static final String KAFKA_ID = "kafka_id";
	private static final String KAFKA_METADATA = "metadata.broker.list";
	private static final String SERIALIZER = "serializer.class";
	private static final String TYPE = "producer.type";
	private static final String REQUIRED_ACKS = "request.required.acks";

	public PropertiesConfiguration kafkaProperties = null;
	public PropertiesConfiguration dcKafkaProperties = null;
	public String environment = null;

	public KafkaUtil(String environment) {
		super();
		this.environment = environment;
		loadKafkaProperties(environment);

	}

	public KafkaUtil(String environment, String dc) {
		super();
		this.environment = environment;
		loadDCKafkaProperties(environment);

	}
	public SpoutConfig getSpoutConfig(String topic, String zkroot,String groupId)
			throws ConfigurationException {
		SpoutConfig spoutConfig = null;
		BrokerHosts hosts = new ZkHosts(kafkaProperties.getString(ZOOKEEPER));
		//String kafka_id = kafkaProperties.getString(KAFKA_ID);
		spoutConfig = new SpoutConfig(hosts, topic, "/" + zkroot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// Temporary test change to see if multiple consumers work
		//spoutConfig.forceFromStart = true;
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return spoutConfig;

	}

	public SpoutConfig getSpoutConfig(String topic, String zkroot)
			throws ConfigurationException {
		SpoutConfig spoutConfig = null;
		BrokerHosts hosts = new ZkHosts(kafkaProperties.getString(ZOOKEEPER));
		String kafka_id = kafkaProperties.getString(KAFKA_ID);
		spoutConfig = new SpoutConfig(hosts, topic, "/" + zkroot, kafka_id);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// Temporary test change to see if multiple consumers work
		//spoutConfig.forceFromStart = true;
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return spoutConfig;

	}
	
	
	public SpoutConfig getSpoutConfig(String topic, String zkroot, boolean readcurrentOnly)
			throws ConfigurationException {
		SpoutConfig spoutConfig = null;
		BrokerHosts hosts = new ZkHosts(kafkaProperties.getString(ZOOKEEPER));
		String kafka_id = kafkaProperties.getString(KAFKA_ID);
		spoutConfig = new SpoutConfig(hosts, topic, "", kafka_id);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return spoutConfig;

	}
	
	
	
	public void sendKafkaMSGs(String message, String currentTopic)
			throws ConfigurationException {

		Producer<String, String> producer = getKafkaProducer();
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(
				currentTopic, "", message);
		producer.send(data);
		producer.close();

	}

	private Producer<String, String> getKafkaProducer()
			throws ConfigurationException {

		if (kafkaProperties != null) {
			Properties properties = new Properties();
			//String kafkaserver = kafkaProperties.getString(KAFKA_METADATA);
			String tempServer=kafkaProperties.getProperty(KAFKA_METADATA).toString();
			String tempServer1 = StringUtils.remove(tempServer, '[');
			String kafkaserver = StringUtils.remove(tempServer1, ']');
			properties.put(KAFKA_METADATA, kafkaserver);
			properties.put(SERIALIZER, kafkaProperties.getProperty(SERIALIZER));
			properties.put(TYPE, "async");
			properties.put(REQUIRED_ACKS, "0");
			ProducerConfig config = new ProducerConfig(properties);
			Producer<String, String> producer = new Producer<String, String>(
					config);
			return producer;
		} else
			throw new ConfigurationException("Kafka properties is not loaded");
	}

	
	
	public PropertiesConfiguration loadKafkaProperties(String environment) {

		try {
			if (environment != null) {
				String propertyurl = null;
				if (PRODUCTION.equals(environment))
					propertyurl = RESOURCE + "/kafka_prod.properties";
				else if (QA.equals(environment))
					propertyurl = RESOURCE + "/kafka_qa.properties";
				else if (LOCAL.equals(environment)) {
					propertyurl = RESOURCE + "/kafka_local.properties";

				}
				if (propertyurl != null) {

					kafkaProperties = new PropertiesConfiguration(propertyurl);
					LOGGER.info("~~~~~~~Using " + environment
							+ " properties in KafkaUtil~~~~~~~~~");

				}

			}
		} catch (ConfigurationException e) {
			LOGGER.error("Error Loading Kafka properties from env : "
					+ environment + " " + e.getMessage());
			e.printStackTrace();
		}

		return kafkaProperties;

	}

	public SpoutConfig getDCSpoutConfig(String topic, String zkroot,String groupId)
			throws ConfigurationException {
		SpoutConfig spoutConfig = null;
		BrokerHosts hosts = new ZkHosts(dcKafkaProperties.getString(ZOOKEEPER));
		spoutConfig = new SpoutConfig(hosts, topic, "/" + zkroot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return spoutConfig;
	}
	
	public SpoutConfig getDCSpoutConfig(String topic, String groupId)
			throws ConfigurationException {
		SpoutConfig spoutConfig = null;
		BrokerHosts hosts = new ZkHosts(dcKafkaProperties.getString(ZOOKEEPER));
		spoutConfig = new SpoutConfig(hosts, topic, "", groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return spoutConfig;
	}

	
	public PropertiesConfiguration loadDCKafkaProperties(String environment) {

		try {
			if (environment != null) {
				String propertyurl = null;
				propertyurl = RESOURCE + "/kafka_dc.properties";
				if (propertyurl != null) {
					dcKafkaProperties = new PropertiesConfiguration(propertyurl);
					LOGGER.info("~~~~~~~Using " + environment
							+ " properties in KafkaUtil~~~~~~~~~");
				}
			}
		} catch (ConfigurationException e) {
			LOGGER.error("Error Loading Kafka properties from env : "
					+ environment + " " + e.getMessage());
			e.printStackTrace();
		}
		return dcKafkaProperties;
	}
	

}
