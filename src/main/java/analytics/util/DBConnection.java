package analytics.util;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class DBConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(DBConnection.class);
	private static MongoClient mongoClient;
	private static String sServerName = "";
	private static String sServerName2 = "";
	private static int sPort = 0;
	//Write concern
	private static int writeconcern = 0;
	private static List<ServerAddress> sServers = new ArrayList<ServerAddress>();
	
	private static String sDatabaseName = "";
	private static String sUserName = "";
	private static String sPassword = "";
	public static DB getDBConnection() throws ConfigurationException{
		return getDBConnection("default");
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
			sServerName2 = properties.getString("server2.name");
			sPort = Integer.parseInt( properties.getString("port.no"));
			//sServerName = properties.getString("server.name");
			
			sDatabaseName = properties.getString("database.name");
			sUserName = properties.getString("user.name");
			sPassword = properties.getString("user.password");
		
			if("server2".equals(server)&&sServerName2!=null&&!sServerName2.isEmpty())
			{
				mongoClient = new MongoClient(sServerName2, sPort);
			}
			else{
				//Following is the logic to implement write concern.
				writeconcern = Integer.parseInt( properties.getString("user.writeconcern"));
				String serverlist = properties.getString("servers.list"); 
				String[] servers = serverlist.split(";");
				for (String serverurl : servers) {
					sServers.add(new ServerAddress(serverurl, sPort));
				}
				
                // Code change to set write options differently
				MongoClientOptions mongoClientOptions= new MongoClientOptions.Builder().writeConcern(new WriteConcern(writeconcern)).build();
				MongoClient mongoClient = new MongoClient(sServers, mongoClientOptions);
		
				
				//mongoClient	= new MongoClient(sServers);
				//mongoClient.setWriteConcern(new WriteConcern(writeconcern));
			}
			/*
			else{
				mongoClient = new MongoClient(sServerName, sPort);
			}
				*/
		} catch (UnknownHostException e) {
			LOGGER.error("Mongo host unknown",e);
		}
	
			conn = mongoClient.getDB(sDatabaseName);
		//	System.out.println("Connection is established...."+ mongoClient.getAllAddress() + " " + conn.getName());
			LOGGER.info("Connection is established...."+ mongoClient.getAllAddress() + " " + conn.getName());
			conn.authenticate(sUserName, sPassword.toCharArray());
			return conn;
	}
	
	
	
	
}
