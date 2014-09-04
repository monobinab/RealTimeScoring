package analytics.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class REDISConnection {
	public static String[] getServers(){
		Properties prop = new Properties();
		try {
		    //load a properties file from class path, inside static method
		    prop.load(REDISConnection.class.getClassLoader().getResourceAsStream("redis_server.properties"));
		    List<String> servers= new ArrayList<String>();
		    int i=1;
		    while(prop.containsKey("server"+i)){
		    	servers.add(prop.getProperty("server"+i));
		    	i++;
		    }
		    return servers.toArray(new String[i-1]);
		} 
		catch (IOException ex) {
		    ex.printStackTrace();
		    return null;
		}
	}
}
