package analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: syermalk
 * Date: 11/9/13
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class StoreZipMap {

    private Map<Integer,Integer> map;

    private static StoreZipMap instance;

    private StoreZipMap() {

    }

    public static StoreZipMap getInstance()
    {
        if (instance == null)
        {
            instance = new StoreZipMap();
            instance.map = new HashMap<Integer, Integer>(3000);

            try {
            	InputStreamReader isr = new InputStreamReader(StoreZipMap
                        .class.getClassLoader()
                        .getResourceAsStream("mainstoreZips.csv"));
                BufferedReader br = new BufferedReader(isr);
                String record = br.readLine();
                while (record != null) {
                    String[] storeZip = record.split(",");
                    instance.map.put(Integer.valueOf(storeZip[0]), Integer.valueOf(storeZip[1]));
                    record = br.readLine();
                }
                isr.close();
                br.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return  instance;
    }

    public Integer getZip(String store)
    {
       return map.get(Integer.valueOf(store));
    }





}
