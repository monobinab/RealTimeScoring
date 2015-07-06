package analytics.spout;

import java.util.ArrayList;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class DCTestSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	String str = null;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("{");
		strBuilder.append("\"strReqDate\": \"2015-06-30\",");
		strBuilder.append("\"strReqTime\": \"14:57:49,572\",");
		strBuilder.append("\"xmlReqData\": \"<?xml version='1.0' encoding='UTF-8'?><soap:Envelope xmlns:soap=\'http://www.w3.org/2003/05/soap-envelope\'><soap:Body><UpdateMemberPrompts xmlns=\'http://www.epsilon.com/webservices/\'><MemberNumber>7081187618793758</MemberNumber><MessageVersion>01</MessageVersion><RequestorID>SWEP</RequestorID><StoreNumber /><Terminal>30316</Terminal>"
				+ "<POSPrompt><POSPromptQuestions><PromptGroupName>DC_WASH</PromptGroupName><QuestionPackageID>BB3300163E00123E11E4211AFED166C0</QuestionPackageID><QuestionRuleID>BB3300163E00123E11E4211AFECF43E1</QuestionRuleID><AttributeID>BB3300163E00123E11E4211AFED118A1</AttributeID><QuestionTextID>BB3300163E00123E11E4211AFED166C0</QuestionTextID>"
				+ "<AnswerChoices><AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3A922F51</AnswerChoiceID><AnswerTxt>Kitchen</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3A925661</AnswerChoiceID><AnswerTxt>Living Room</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA2AA10</AnswerChoiceID><AnswerTxt>Dining Room</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA2D120</AnswerChoiceID><AnswerTxt>TV Room</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA2D121</AnswerChoiceID><AnswerTxt>Basement</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA2F830</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA31F40</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA31F41</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA34650</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA36D60</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AA36D61</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AB324D0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AB34BE0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AB34BE1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AB372F0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AB3C110</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AB3E820</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AB3E821</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AC7BE40</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AD41A50</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AE0EB90</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AE0EB91</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AE1AEE0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AE1D5F0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AE1D5F1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AE1FD00</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AE22410</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AF64850</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AF66F60</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AF66F61</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AF69670</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AF6E490</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AF732B0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3AF759C0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B056380</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B19FCF0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B26F540</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B26F541</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B271C50</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B271C51</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B274360</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B274361</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B276A70</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B279180</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B279181</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B3C2AF1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B3C5200</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B3C5201</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B3C7910</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B3CA021</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B3CC731</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B3CEE40</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B4A5BC0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B581761</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B583E70</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B66BD61</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B66E470</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B758A70</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B758A71</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B834610</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B836D20</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B836D21</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B839430</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B83BB40</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B83BB41</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B840960</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B9101B0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B9D0FA1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B9D36B0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B9D36B1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B9D5DC0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3B9E2110</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BB356C0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BB41A11</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BC30E30</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BC30E31</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BC33541</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BC35C51</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BC38360</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BC38361</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BC3AA70</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BD07BB0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BDD25E0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BED7991</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BEDA0A1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BEDC7B0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BEE3CE0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BFB8351</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BFBAA60</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BFBAA61</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BFBD171</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BFBF880</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BFBF881</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3BFC46A0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C0BD700</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C18A841</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C257980</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C257981</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C25A091</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C2615C0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C4388D0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C34E2D1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C3509E0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C3509E1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C3530F0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C3530F1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C355800</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C357F11</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C52F220</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C52F221</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C531930</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C534040</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C536750</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C62A990</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C62A991</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C62D0A0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C62D0A1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C62F7B0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C62F7B1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C6345D0</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C6345D1</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C780650</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C780651</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C782D60</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C785471</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C787B80</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C787B81</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C78A290</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C86FA70</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C935680</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3C9F3D60</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "<AnswerOption><AnswerChoiceID>BB3300163E00123E11E4211B3CAB9970</AnswerChoiceID><AnswerTxt>Office</AnswerTxt></AnswerOption>"
				+ "</AnswerChoices></POSPromptQuestions></POSPrompt></UpdateMemberPrompts></soap:Body></soap:Envelope>\",");
		strBuilder.append("\"strServerName\":\"trprtel2app12\",");
		strBuilder.append("\"strRespDate\":\"2015-06-30\",");
		strBuilder.append("\"strRespTime\":\"14:57:49,640\"");
		strBuilder.append("}");
		str = strBuilder.toString();
		
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		 ArrayList<Object> listToEmit = new ArrayList<Object>();
	        listToEmit.add(str);
		this.collector.emit(listToEmit);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("str"));
		
	}

}