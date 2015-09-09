package cpstest;

import analytics.util.CPSUtil;

public class CPSTester {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
        // INPUT and file name
		if(args==null||args.length==0)
		System.exit(0);	
		String testPhase=args[0];
		String filename=args[1];
		CPSUtil cpsUtil=new CPSUtil();	
		cpsUtil.processFile(filename, testPhase);
		
	}
	

}
