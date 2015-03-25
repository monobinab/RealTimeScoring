package analytics.util;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;




public class PropertiesFileReader {

	/**
	 * Apache Log4j Instance
	 */
	private static Logger logger = Logger.getLogger(PropertiesFileReader.class);

	/**
	 * The class name of this class to be used in logging.
	 */

	public PropertiesFileReader() {

	}

	public static PropertiesConfiguration loadPropFile(String fileName) {
		logger.debug("Entry :PropertiesFileReader:loadPropFile");
		try {
			return new PropertiesConfiguration("resources/"+fileName);
		} catch (Exception ex) {
			logger.error("Exeption in loadPropFile>>"+ex.getMessage());
		} 
		return null;
	}

	public static String getProperty(String strKey, PropertiesConfiguration props) {
		System.out.println(props.getString(strKey));
		return props.getString(strKey);
	}

}