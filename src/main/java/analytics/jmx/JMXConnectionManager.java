package analytics.jmx;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JMXConnectionManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(JMXConnectionManager.class);
	  
	private static JMXConnectionManager jmxConnectionManager;
	
	private static AppMetricsBean appMetricsBean = null;
	
	private static AtomicInteger atomicInteger = null;
	
	private JMXConnectionManager(){}
	
	 /**
     * Create a static method to get instance.
     */
    public static JMXConnectionManager getInstance(){
        if(jmxConnectionManager == null){
        	jmxConnectionManager = new JMXConnectionManager();
        	atomicInteger = new AtomicInteger();
        	register();
        }
        return jmxConnectionManager;
    }
    
    private static void register(){
    	MBeanServer mbServer = ManagementFactory.getPlatformMBeanServer();
    	if(mbServer != null){
    		try{
	    		appMetricsBean = new AppMetricsBean();
	    		final StandardMBean stdMbean = new StandardMBean(appMetricsBean, AppMetricsBeanIntf.class);
	    		ObjectName objName = new ObjectName("com.monitoring:type=appMetricsBean");
	    		if(objName != null){
	    			mbServer.registerMBean(stdMbean, objName);
	    		}
    		}catch(Exception ex){
    			LOGGER.error("Failed to register mbean, graphite metrices will not be available - " + ex.getMessage());
    			System.out.println("@@@@@@@@@@@ ---->> ---->> Failed to register mbean, graphite metrices will not be available - " + ex.getMessage());
    		}
    	}
    }
    
    public AppMetricsBean getAppMetricsBean(){
    	return appMetricsBean;
    }
    
    public AtomicInteger getAtomicIntegerInstance(){
    	return atomicInteger;
    }
}
