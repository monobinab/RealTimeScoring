/**
 * 
 */
package analytics.util.objects;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author spannal
 *
 */
public class RtsCommonObj implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String lyl_id_no;
	ArrayList<String> pidList;
	Long time;
	
	/**
	 * @return the time
	 */
	public Long getTime() {
		return time;
	}
	/**
	 * @param time the time to set
	 */
	public void setTime(Long time) {
		this.time = time;
	}
	/**
	 * @return the pidList
	 */
	public ArrayList<String> getPidList() {
		return pidList;
	}
	/**
	 * @param pidList the pidList to set
	 */
	public void setPidList(ArrayList<String> pidList) {
		this.pidList = pidList;
	}
	public String getLyl_id_no() {
		return lyl_id_no;
	}
	public void setLyl_id_no(String lyl_id_no) {
		this.lyl_id_no = lyl_id_no;
	}


	

}
