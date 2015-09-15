package cpstest;

import analytics.util.CPSUtil;


public class CPSTester {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
        // INPUT and file name
		if(args==null||args.length==0)
		System.exit(0);	
		
		String presetFile=args[0];
		String testFile=args[1];
		String verifyFile=args[2];

		CPSUtil cpsUtil=new CPSUtil();	
		cpsUtil.processFile(presetFile, testFile,verifyFile);

		
	}
	

}
