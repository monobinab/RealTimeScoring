package analytics.util;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
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
	//Kafka related constants
	private static final String ZOOKEEPER = "zookeeper";
	private static final String KAFKA_ID = "kafka_id";	
	private static final String KAFKA_METADATA="metadata.broker.list";
	private static final String SERIALIZER="serializer.class";
	
	
	public static PropertiesConfiguration kafkaProperties = null;
	public static SpoutConfig spoutConfig = null;

	public static PropertiesConfiguration loadKafkaProperties(String environment)
			throws ConfigurationException {

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

		return kafkaProperties;
	}

	public static SpoutConfig getSpoutConfig(String environment, String topic)
			throws ConfigurationException {

		if (spoutConfig == null) {
			getKafkaProperties(environment);
			BrokerHosts hosts = new ZkHosts(
					kafkaProperties.getString(ZOOKEEPER));
			String kafka_id = kafkaProperties.getString(KAFKA_ID);
			spoutConfig = new SpoutConfig(hosts, topic, "", kafka_id);
			spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		}

		return spoutConfig;

	}

	public static Producer getKafkaProducer() throws ConfigurationException {

		if (kafkaProperties != null) {
			Properties properties = new Properties();
			String kafkaserver = kafkaProperties.getString(KAFKA_METADATA);
			properties.put(KAFKA_METADATA, kafkaserver);
			properties.put(SERIALIZER,
					kafkaProperties.getProperty(SERIALIZER));
			ProducerConfig config = new ProducerConfig(properties);
			Producer<String, String> producer = new Producer<String, String>(
					config);
			return producer;
		} else
			throw new ConfigurationException("Kafka properties is not loaded");
	}

	public static void getKafkaProperties(String environment)
			 {
		if (kafkaProperties == null) {
			try {
				loadKafkaProperties(environment);
			} catch (ConfigurationException e) {
				LOGGER.error("Unable to load Kafka Properties");
				e.printStackTrace();
			}
		}
	}

}
