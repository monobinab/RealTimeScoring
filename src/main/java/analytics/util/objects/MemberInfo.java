package analytics.util.objects;

public class MemberInfo {
	
		private String eid;
		private String emailOptIn;
		private String state;
		private String srs_opt_in;
		private String kmt_opt_in;
		private String syw_opt_in;
		
		/**
		 * @return the srs_opt_in
		 */
		public String getSrs_opt_in() {
			return srs_opt_in;
		}
		/**
		 * @param srs_opt_in the srs_opt_in to set
		 */
		public void setSrs_opt_in(String srs_opt_in) {
			this.srs_opt_in = srs_opt_in;
		}
		/**
		 * @return the kmt_opt_in
		 */
		public String getKmt_opt_in() {
			return kmt_opt_in;
		}
		/**
		 * @param kmt_opt_in the kmt_opt_in to set
		 */
		public void setKmt_opt_in(String kmt_opt_in) {
			this.kmt_opt_in = kmt_opt_in;
		}
		/**
		 * @return the syw_opt_in
		 */
		public String getSyw_opt_in() {
			return syw_opt_in;
		}
		/**
		 * @param syw_opt_in the syw_opt_in to set
		 */
		public void setSyw_opt_in(String syw_opt_in) {
			this.syw_opt_in = syw_opt_in;
		}
		public String getState() {
			return state;
		}
		public void setState(String state) {
			this.state = state;
		}
		public String getEmailOptIn() {
			return emailOptIn;
		}
		public void setEmailOptIn(String emailOptIn) {
			this.emailOptIn = emailOptIn;
		}
		public String getEid() {
			return eid;
		}
		public void setEid(String eid) {
			this.eid = eid;
		}
		
}
