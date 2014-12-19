package analytics.util;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class DCParserHandler extends DefaultHandler {
	boolean bq_id = false;
	boolean ba_id = false;
	boolean bp_id = false;
	boolean bm_id = false;
	boolean b_num = false;
	String q_id = null;
	String a_id = null;
	String promptGroupName = null;
	String memberId = null;
	List<JSONObject> answers = new ArrayList<JSONObject>();
	public String num = null;

	int bq = 0;
	int ba = 0;
	int bp = 0;
	int bm = 0;
	
	public void startDocument() throws SAXException{
		q_id = null;
		a_id = null;
		promptGroupName = null;
		memberId = null;
		answers.clear();
	}

	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

		if (qName.contains("QuestionTextID") && !qName.contains("FollowupQuestionTextID")) {
			bq_id = true;
		}

		if (qName.contains("AnswerID") && !qName.contains("FollowupAnswerID")) {
			ba_id = true;
		}

		if (qName.contains("PromptGroupName")) {
			bp_id = true;
		}

		if (qName.contains("MemberNumber")) {
			bm_id = true;
		}
		
		if(qName.contains("NumQuestionsReturned")){
			b_num = true;
		}
	}

	public void characters(char ch[], int start, int length) throws SAXException {
		String str = new String(ch, start, length);
		if (bq_id) {
			q_id = str;// "bb3300163e00123e11e4211b3aa234e0";
			bq_id = false;
			bq++;
		}

		if (ba_id) {
			a_id = str; // "bb3300163e00123e11e4211b3aa34650";
			ba_id = false;
			ba++;
		}

		if (bp_id) {
			promptGroupName = str; // "DC_Appliance";
			bp_id = false;
			bp++;
		}

		if (bm_id) {
			memberId = str;
			bm_id = false;
			bm++;
		}
		
		if(b_num){
			num = str;
			b_num = false;
		}

		if (q_id != null && a_id != null && promptGroupName != null && memberId != null) {
			JSONObject obj = new JSONObject();
			try {
				obj.put("promptGroupName", promptGroupName);
				obj.put("memberId", memberId);
				obj.put("q_id", q_id);
				obj.put("a_id", a_id);
				answers.add(obj);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			clear();
		}
	}
	
	public void endElement(String uri, String localName, String qName){
		bq_id = false;
		ba_id = false;
		bp_id = false;
		bm_id = false;
	}

	public void clear() {
		q_id = null;
		a_id = null;
		bq_id = false;
		ba_id = false;
	}

	public void endDocument() throws SAXException {
		ba = 0;
		bq = 0;
	}
	
	public List<JSONObject> getAnswerList(){
		return answers;
	}
	
	public String getMemberId(){
		return memberId;
	}
}
