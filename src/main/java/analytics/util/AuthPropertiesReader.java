package analytics.util;

import java.util.Properties;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;




public class AuthPropertiesReader extends PropertiesFileReader {

	/**
	 * Apache Log4j Instance
	 */
	private static Logger logger = Logger.getLogger(AuthPropertiesReader.class);

	/**
	 * The class name of this class to be used in logging.
	 */

	 //public static Properties props = null;
	static PropertiesConfiguration props=null;
	public static String getProperty(String strKey) {
		
		if (props == null) {
			props = loadPropFile("authentication.properties");
		}
		logger.debug("Exit: AuthPropertiesReader:getProperty>>getAuthProperty");
		return getProperty(strKey, props);
	}

}
