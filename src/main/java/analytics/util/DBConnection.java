package analytics.util;

import java.net.UnknownHostException;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class DBConnection {

	static final Logger logger = Logger.getLogger(DBConnection.class);
	private MongoClient mongoClient;
	private String sServerName = "";
	private int sPort = 0;
	private String sDatabaseName = "";
	private String sUserName = "";
	private String sPassword = "";

	public DB getDBConnection() throws Exception {
		DB conn = null;

		PropertiesConfiguration properties = new PropertiesConfiguration("./src/main/resources/connection_config.properties");
						
		sServerName = properties.getString("server.name");
		sPort = Integer.parseInt( properties.getString("port.no"));
		sDatabaseName = properties.getString("database.name");
		sUserName = properties.getString("user.name");
		sPassword = properties.getString("user.password");
		
		try {
			mongoClient = new MongoClient(sServerName, sPort);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		conn = mongoClient.getDB(sDatabaseName);
		logger.info("Connection is established...."+conn.getName());
		conn.authenticate(sUserName, sPassword.toCharArray());
		return conn;
	}

	/*public DB getDBConnectionWithoutCredentials() throws Exception {
		DB conn = null;

		PropertiesConfiguration properties = new PropertiesConfiguration("./src/main/resources/connection_config.properties");
						
		sServerName = properties.getString("server.name");
		sPort = Integer.parseInt( properties.getString("port.no"));
		sDatabaseName = properties.getString("database.name");
		sUserName = properties.getString("user.name");
		sPassword = properties.getString("user.password");
		
		try {
			mongoClient = new MongoClient(sServerName, sPort);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		conn = mongoClient.getDB(sDatabaseName);
		logger.info("Connection is established...."+conn.getName());
		//conn.authenticate(sUserName, sPassword.toCharArray());
		return conn;
	}*/

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public String getsServerName() {
		return sServerName;
	}

	public void setsServerName(String sServerName) {
		this.sServerName = sServerName;
	}

	public int getsPort() {
		return sPort;
	}

	public void setsPort(int sPort) {
		this.sPort = sPort;
	}

	public String getsDatabaseName() {
		return sDatabaseName;
	}

	public void setsDatabaseName(String sDatabaseName) {
		this.sDatabaseName = sDatabaseName;
	}

	public String getsUserName() {
		return sUserName;
	}

	public void setsUserName(String sUserName) {
		this.sUserName = sUserName;
	}

	public String getsPassword() {
		return sPassword;
	}

	public void setsPassword(String sPassword) {
		this.sPassword = sPassword;
	}

}
