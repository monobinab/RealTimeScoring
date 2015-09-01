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
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class DBConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(DBConnection.class);
	//private static MongoClient mongoClient;
	private static String sServerName = "";
	private static String sServerName2 = "";
	private static String sServerName2_2 = "";
	private static int sPort = 0;
	//Write concern
	private static int writeconcern = 0;
	private static int writeconcern2 = 0;
	
	private static String sDatabaseName = "";
	private static String sUserName = "";
	private static String sPassword = "";
	
	//Connection variables for connecting to Mongo2 incase of MemberVariables
	private static String sDatabaseName2_2 = "";
	private static String sUserName2_2 = "";
	
	
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
			
			//Code to connect to Mongo2 incase of MemberVariables
			sServerName2_2 = properties.getString("server2_2.name");
			sDatabaseName2_2 = properties.getString("database2_2.name");
			sUserName2_2 = properties.getString("user.name2_2");
			
			//Connection to Mongo Server 2
			if("server2_2".equalsIgnoreCase(server) && sServerName2_2!=null&&!sServerName2_2.isEmpty()){
				MongoClient mongoClient;
				String[] servers2_2 = sServerName2_2.split(";");
				List<ServerAddress> sServers2_2 = new ArrayList<ServerAddress>();
				for (String serverurl2_2 : servers2_2) {
					sServers2_2.add(new ServerAddress(serverurl2_2, sPort));
				}
				mongoClient = MongoConnectionHelper.getMongoClientProd2_2(sServers2_2);
				conn = mongoClient.getDB(sDatabaseName2_2);
				LOGGER.info("Connection is established ...."+ mongoClient.getAllAddress() + " " + conn.getName());
				conn.authenticate(sUserName2_2, sPassword.toCharArray());
				return conn;
			}
		
			//Connection to Mongo Server 8,12 and 13
			if("server2".equals(server)&&sServerName2!=null&&!sServerName2.isEmpty())
			{
				MongoClient mongoClient;
				String serverlist2 = properties.getString("server2.list"); 
				List<ServerAddress> sServers2 = new ArrayList<ServerAddress>();
				writeconcern2 = Integer.parseInt( properties.getString("server2.user.writeconcern"));
				String[] servers2 = serverlist2.split(";");
				for (String serverurl2 : servers2) {
					sServers2.add(new ServerAddress(serverurl2, sPort));
				}
				mongoClient	= MongoConnectionHelper.getMongoClientProd2(sServers2);
				mongoClient.setWriteConcern(new WriteConcern(writeconcern2,100));
				
				conn = mongoClient.getDB(sDatabaseName);
				LOGGER.info("Connection is established...."+ mongoClient.getAllAddress() + " " + conn.getName());
				conn.authenticate(sUserName, sPassword.toCharArray());
				return conn;
			}
			
			//Connection to Mongo Server 7,9 and 11
			else{
				MongoClient mongoClient;
				String serverlist = properties.getString("servers.list"); 
				//Following is the logic to implement write concern.
				List<ServerAddress> sServers = new ArrayList<ServerAddress>();
				writeconcern = Integer.parseInt( properties.getString("user.writeconcern"));
				String[] servers = serverlist.split(";");
				for (String serverurl : servers) {
					sServers.add(new ServerAddress(serverurl, sPort));
				}
				
                // Code change to set write options differently
				//MongoClientOptions mongoClientOptions= new MongoClientOptions.Builder().writeConcern(new WriteConcern(writeconcern)).build();
				//MongoClient mongoClient = new MongoClient(sServers, mongoClientOptions);
						
				mongoClient	= MongoConnectionHelper.getMongoClientProd1(sServers);
				//Setting the write concern timeout to 100 milli seconds
				//After waiting for 100 milliseconds, each write will throw an exception 
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
