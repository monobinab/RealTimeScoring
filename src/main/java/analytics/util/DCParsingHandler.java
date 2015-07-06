package analytics.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.objects.ParsedDC;

public class DCParsingHandler {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DCParsingHandler.class);

	public static ParsedDC getAnswerJson(String message) {
		String tagContent = null;
		boolean updateMemberPromptsFlag = false;
		ParsedDC parsedDC = null;
		List<String> answerChoiceIds = null;
		String promptGroupName = null;
		XMLInputFactory factory = XMLInputFactory.newInstance();

		try {
			XMLStreamReader reader = factory
					.createXMLStreamReader(new StringReader(message));

			while (reader.hasNext()) {
				int event = reader.next();
				switch (event) {
			   case XMLStreamConstants.START_ELEMENT:

					if ("UpdateMemberPrompts".equals(reader.getLocalName())) {
						updateMemberPromptsFlag = true;
						parsedDC = new ParsedDC();
						answerChoiceIds = new ArrayList<String>();
						break;
					}
			
				break;

				case XMLStreamConstants.CHARACTERS:
					tagContent = reader.getText().trim();
				break;

				case XMLStreamConstants.END_ELEMENT:
					
					if(updateMemberPromptsFlag){
	
						if ("AnswerChoiceID".equals(reader.getLocalName()) && tagContent != null) {
							tagContent = tagContent.toLowerCase();
							answerChoiceIds.add(tagContent);
							/*if(promptGroupName != null){
								LOGGER.info("promptGroup " + promptGroupName + "answerChoiceId " + tagContent);
								//System.out.println(promptGroupName +" " + tagContent);
							}*/
							break;
						}
						if ("POSPrompt".equals(reader.getLocalName())) {
							parsedDC.setAnswerChoiceIds(answerChoiceIds);
							break;
						}
						if ("CheckoutPrompt".equalsIgnoreCase(reader.getLocalName())) {
							parsedDC.setAnswerChoiceIds(answerChoiceIds);
							break;
						}
						if ("MemberNumber".equals(reader.getLocalName()) && tagContent != null) {
							parsedDC.setMemberId(tagContent);
							break;
						}
						/*if("PromptGroupName".equals(reader.getLocalName()) && tagContent != null && tagContent.equalsIgnoreCase("DC_CE")){
							promptGroupName = tagContent;
							break;
						}*/
				 break;
					}
				}
			}
			LOGGER.debug("member " + parsedDC.getMemberId() + " answers " + parsedDC.getAnswerChoiceIds().size());
			System.out.println(parsedDC.getMemberId() + " " + parsedDC.getAnswerChoiceIds().size());
			
		} catch (Exception e1) {
			LOGGER.error(e1.getClass() + ": " + e1.getMessage(), e1);
		} 
		return parsedDC;
	}
}