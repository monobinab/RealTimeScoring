package analytics.util;


import analytics.util.dao.DivLnItmDao;
import analytics.util.dao.PidDivLnDao;
import analytics.util.objects.DivLn;

public class PidMatchUtils{
	private PidDivLnDao pidDivLn;
	private DivLnItmDao divLnItm;
	public PidMatchUtils() {
		pidDivLn = new PidDivLnDao();
		divLnItm = new DivLnItmDao();
		
	}
	public DivLn getDivInformation(String pid){
		String div;
		String divLn;
		String item;
		pid = pid.trim().toUpperCase();//maybe we can remove this
		if(pid.startsWith("S")||pid.length()>17){
			return null;
			//market place item -  we can not process it
		}
		//If pid does not end with P, we need to add it
		if((pid.length()==11||pid.length()==16) && !pid.endsWith("P")){
			pid = pid+'P';
		}
		DivLn divLnObj = pidDivLn.getDivLnFromPid(pid);
		if(divLnObj!=null){
			return divLnObj;
		}
		
		if(pid.length()==12){
			//If PID length equals 12, then it is a Sears product and div and itm_no can be parsed out
			div = pid.substring(0, 3);
			item = pid.substring(3, 8);
			/*divLn = div + divLnItm.getLnFromDivItem(div, item);
			return new DivLn(div, divLn); */
			DivLn divLnObj2 = divLnItm.getLnFromDivItemTag(div, item);
			return divLnObj2;
		}
		
		if(pid.length()==14 && pid.contains("CO")){
			div = pid.substring(0, 2);
			return new DivLn(div, div);
		}
		if(pid.length()==17){
			div= pid.substring(0,2);
			if(pid.contains("V")||pid.contains("W")){
				item = pid.substring(4,12);//kmart item
				divLn = div + divLnItm.getLnFromDivItem(div, item);
				return new DivLn(div, divLn);
			}
		}
		return null;
	}
}
