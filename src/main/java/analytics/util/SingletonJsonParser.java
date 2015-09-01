package analytics.util;

import com.google.gson.Gson;

public class SingletonJsonParser {
	
	private static SingletonJsonParser singletonJsonParser;
	
	private Gson gson = new Gson();
	
    /**
     * Create private constructor
     */
    private SingletonJsonParser(){
         
    }
    /**
     * Create a static method to get instance.
     */
    public static SingletonJsonParser getInstance(){
        if(singletonJsonParser == null){
        	singletonJsonParser = new SingletonJsonParser();
        }
        return singletonJsonParser;
    }
     
    public Gson getGsonInstance(){
        return gson;
    }
}
