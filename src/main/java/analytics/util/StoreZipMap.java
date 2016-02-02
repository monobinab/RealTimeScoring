package analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.VariableDao;

/**
 * Created with IntelliJ IDEA.
 * User: syermalk
 * Date: 11/9/13
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class StoreZipMap {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StoreZipMap.class);
    private Map<Integer,Integer> map;

    private static StoreZipMap instance;

    private StoreZipMap() {

    }

    public static StoreZipMap getInstance()
    {
        if (instance == null)
        {
            /*instance = new StoreZipMap();
            instance.map = new HashMap<Integer, Integer>(3000);*/
        	
        	StoreZipMap storeZipMap  = new StoreZipMap();
        	storeZipMap.map = new HashMap<Integer, Integer>(3000);
            
            try {
            	InputStreamReader isr = new InputStreamReader(StoreZipMap
                        .class.getClassLoader()
                        .getResourceAsStream("mainstoreZips.csv"));
                BufferedReader br = new BufferedReader(isr);
                String record = br.readLine();
                while (record != null) {
                    String[] storeZip = record.split(",");
                    storeZipMap.map.put(Integer.valueOf(storeZip[0]), Integer.valueOf(storeZip[1]));
                    record = br.readLine();
                }
                instance = storeZipMap;
                isr.close();
                br.close();
            } catch (IOException e) {
                LOGGER.warn("Unable to read store zip mappings",e);
            }
        }
        return  instance;
    }

    public Integer getZip(String store)
    {
       return map.get(Integer.valueOf(store));
    }





}
