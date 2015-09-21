package analytics.util;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLDataSource {
	private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDataSource.class);

    private static MySQLDataSource     datasource;
    private BasicDataSource ds;

    private MySQLDataSource() throws IOException, SQLException, PropertyVetoException, ConfigurationException {
    	PropertiesConfiguration properties = null;
        ds = new BasicDataSource();
           
        String isProd = System.getProperty(MongoNameConstants.IS_PROD);
	
		if(isProd!=null && "PROD".equals(isProd)){
			properties=  new PropertiesConfiguration("resources/mysql_connection_prod.properties");
			LOGGER.info("~~~~~~~Using production properties in MySQLDataSource~~~~~~~~~");
		}
		
		else if(isProd!=null && "QA".equals(isProd)){
			properties=  new PropertiesConfiguration("resources/mysql_connection_qa.properties");
			LOGGER.info("Using QA properties in MySQLDataSource");	
		}
		
		else if(isProd!=null && "LOCAL".equals(isProd)){
			properties=  new PropertiesConfiguration("resources/mysql_connection_local.properties");
			LOGGER.info("Using DEV properties in MySQLDataSource");	
		}
		ds.setDriverClassName(properties.getString("driverClass")); 
		ds.setUrl(properties.getString("jdbcUrl"));
		ds.setUsername(properties.getString("username"));
		ds.setPassword(properties.getString("password"));
	}

    public static MySQLDataSource getInstance() throws IOException, SQLException, PropertyVetoException, ConfigurationException {
        if (datasource == null) {
            datasource = new MySQLDataSource();
            return datasource;
        } else {
            return datasource;
        }
    }

    public Connection getConnection() throws SQLException {
        return this.ds.getConnection();
    }

}