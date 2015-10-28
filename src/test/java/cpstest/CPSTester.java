package cpstest;

import analytics.util.CPSUtil;
import analytics.util.MongoNameConstants;


public class CPSTester {

	public static void main(String[] args) {
		// args - input file names and topic name
		if(args==null||args.length==0)
		System.exit(0);	
		
		String presetFile=args[0];
		String testFile=args[1];
		String verifyFile=args[2];
		String testresults=args[3];
		String topicName=args[4];
		String env = args[5];
		System.setProperty(MongoNameConstants.IS_PROD, env);
		CPSUtil cpsUtil=new CPSUtil();	
		cpsUtil.processFile(presetFile, testFile,verifyFile,testresults,topicName);
		
	}
	

}
