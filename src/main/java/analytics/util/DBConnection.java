package analytics.util;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class DBConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(DBConnection.class);
	private static MongoClient mongoClient;
	private static String sServerName = "";
	private static String sServerName2 = "";
	private static int sPort = 0;
	private static String sDatabaseName = "";
	private static String sUserName = "";
	private static String sPassword = "";
	public static DB getDBConnection() throws ConfigurationException{
		return getDBConnection("default");
	}
	public static DB getDBConnection(String server) throws ConfigurationException {
		DB conn = null;
		PropertiesConfiguration properties = null;
		String isProd = System.getProperty(MongoNameConstants.IS_PROD);
		//If test, return only a test fake mongo connection
		if(isProd!=null && "test".equals(isProd)){
			return FakeMongo.getTestDB();
		}
		//TODO: Hard coding prod
		//isProd = "true";

		if(isProd!=null && "PROD".equals(isProd)){
			properties=  new PropertiesConfiguration("resources/connection_config_prod.properties");
			LOGGER.info("Using production properties");
		}
		else if(isProd!=null && "QA".equals(isProd)){
			properties=  new PropertiesConfiguration("resources/connection_config.properties");
			LOGGER.info("Using test properties");	
		}		

		sServerName = properties.getString("server.name");
		sServerName2 = properties.getString("server2.name");
		sPort = Integer.parseInt( properties.getString("port.no"));
		sDatabaseName = properties.getString("database.name");
		sUserName = properties.getString("user.name");
		sPassword = properties.getString("user.password");
		
		try {
			if("server2".equals(server)&&sServerName2!=null&&!sServerName2.isEmpty())
			{
				mongoClient = new MongoClient(sServerName2, sPort);
			}
			else{
				mongoClient = new MongoClient(sServerName, sPort);
			}
		} catch (UnknownHostException e) {
			LOGGER.error("Mongo host unknown",e);
		}

		conn = mongoClient.getDB(sDatabaseName);
		LOGGER.info("Connection is established...."+ mongoClient.getAddress() + " " + conn.getName());
		conn.authenticate(sUserName, sPassword.toCharArray());
		return conn;
	}
}
