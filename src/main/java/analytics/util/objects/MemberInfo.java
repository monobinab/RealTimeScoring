package analytics.util.objects;

public class MemberInfo {
	
		private String eid;
		private String emailOptIn;
		private String state;
		
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
