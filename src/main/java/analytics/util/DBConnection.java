package analytics.util;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class DBConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(DBConnection.class);

	private static int sPort = 0;
	private static int writeconcern = 0;
	private static String sDatabaseName = "";
	private static String sUserName = "";
	private static String sPassword = "";
	private static String serversStr = "";

	
	
	public static DB getDBConnection() throws ConfigurationException{
		return getDBConnection("dynamic");
	}

	public static DB getDBConnection(String server) throws ConfigurationException {
		LOGGER.info("~~~~~~~~~~~~~~~DBCONNECTION CLASS~~~~~~~: " + System.getProperty(MongoNameConstants.IS_PROD));
		DB conn = null;
		PropertiesConfiguration properties = null;
		String isProd = System.getProperty(MongoNameConstants.IS_PROD);
		//If test, return only a test fake mongo connection
		if(isProd!=null && "test".equals(isProd)){
			return FakeMongo.getTestDB();
		}
	
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

		try {
			
			sPort = Integer.parseInt( properties.getString(server+".port.no"));
			sDatabaseName = properties.getString(server+".database.name");
			sUserName = properties.getString(server+".user.name");
			sPassword = properties.getString(server+".user.password");			
			serversStr = properties.getString(server+".replicaset.list"); 
			writeconcern = Integer.parseInt( properties.getString(server+".servers.writeconcern"));	
			
			MongoClient mongoClient;
			List<ServerAddress> serversList = new ArrayList<ServerAddress>();
			if(StringUtils.isNotBlank(serversStr)){
				String[] servers = serversStr.split(";");
				for (String serverUrl : servers) {
					serversList.add(new ServerAddress(serverUrl, sPort));
				}
				
				if(server.equals("dynamic")){
					//to get connection to dynamic replica set
					mongoClient	= MongoConnectionHelper.getMongoClientProd1(serversList);					
				}else if(server.equals("static")){
					//to get connection to static replica set
					mongoClient	= MongoConnectionHelper.getMongoClientProd2(serversList);
				}else {
					//to get connection to mongo2
					mongoClient	= MongoConnectionHelper.getMongoClientProd2_2(serversList);
				}
				
				mongoClient.setWriteConcern(new WriteConcern(writeconcern,100));
				
				conn = mongoClient.getDB(sDatabaseName);
				LOGGER.info("Connection is established...."+ mongoClient.getAllAddress() + " " + conn.getName());
				conn.authenticate(sUserName, sPassword.toCharArray());
				return conn;
				
			}			

		} catch (UnknownHostException e) {
			LOGGER.error("Mongo host unknown",e);
		}
		//Never reached here
		return null;
	}
	
}
