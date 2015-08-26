package analytics.util;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.dbcp.BasicDataSource;

public class MySQLDataSource {

    private static MySQLDataSource     datasource;
    private BasicDataSource ds;

    private MySQLDataSource() throws IOException, SQLException, PropertyVetoException, ConfigurationException {
    	PropertiesConfiguration properties = new PropertiesConfiguration("resources/mysqldatabase.properties");
        ds = new BasicDataSource();
        ds.setDriverClassName(properties.getString("driverClass")); //loads the jdbc driver
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