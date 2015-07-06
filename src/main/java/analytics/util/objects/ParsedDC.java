package analytics.util.objects;

import java.util.List;

public class ParsedDC {
	
	private String memberId;
	private List<String> answerChoiceIds;
	public String getMemberId() {
		return memberId;
	}
	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}
	public List<String> getAnswerChoiceIds() {
		return answerChoiceIds;
	}
	public void setAnswerChoiceIds(List<String> answerChoiceIds) {
		this.answerChoiceIds = answerChoiceIds;
	}

}