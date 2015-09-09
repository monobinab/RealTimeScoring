package analytics.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;






import org.apache.commons.configuration.ConfigurationException;

import cpstest.CPOutBoxItem;
import analytics.util.KafkaUtil;
import analytics.util.dao.CPOutBoxDAO;

public class CPSUtil {

	public void processFile(String filename, String testPhase) {
		FileReader fileReader = null;
		String currentTopic = "rts_cp_membertags";
		String outputFile = "C:\\CPTest\\testresults_"+System.currentTimeMillis()+".txt";
		File result =new File(outputFile);
		try {
			fileReader = new FileReader(filename);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			System.exit(0);
		}
		BufferedReader buffReader = new BufferedReader(fileReader);
		int count = 0;
		String line = new String();
		PrintWriter printWriter = null;
		if ("VERIFY".equalsIgnoreCase(testPhase))
			try {
				printWriter = new PrintWriter(result, "UTF-8");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		try {
			while ((line = buffReader.readLine()) != null) {
				count++;
				try {
					CPOutBoxItem cpOutBoxItem = parseLine(line, testPhase);
					List<String> mdTags = new ArrayList<String>();
					if ("PRESET".equalsIgnoreCase(testPhase)) {
						// Insert entry to the database
						new CPOutBoxDAO().insertRow(cpOutBoxItem);

					} else if ("TEST".equalsIgnoreCase(testPhase)) {
						//create a json and push to kafka topic
						mdTags.add(cpOutBoxItem.getMd_tag());
						String kafkaMSG = createJson(cpOutBoxItem.getLoy_id(),
								mdTags);
						new KafkaUtil("PROD").sendKafkaMSGs(kafkaMSG, currentTopic);

					} else if ("VERIFY".equalsIgnoreCase(testPhase)) {
						//Compare the input data with the one in database to determine success or failure
						cpOutBoxItem.setStatus(0);
						CPOutBoxItem queuedItem = new CPOutBoxDAO()
								.getQueuedItem(cpOutBoxItem.getLoy_id(),
										cpOutBoxItem.getMd_tag(),
										cpOutBoxItem.getStatus());
						String testresult = null;
						if (queuedItem.getSend_date().equalsIgnoreCase(
								cpOutBoxItem.getSend_date())) {
							testresult = "success:" + cpOutBoxItem.getLoy_id()
									+ ", " + cpOutBoxItem.getMd_tag() + ", "
									+ cpOutBoxItem.getSend_date() + "";
						} else {

							testresult = "failure:" + cpOutBoxItem.getLoy_id()
									+ ", " + cpOutBoxItem.getMd_tag() + ", "
									+ cpOutBoxItem.getSend_date() + " vs "
									+ queuedItem.getSend_date();
						}

						printWriter.println(testresult);
						printWriter.flush();

					}

				} catch (ConfigurationException ce) {
					System.err.println("Failed to send messages to Kafka ");
					ce.printStackTrace();
				} catch (Exception e) {
					System.err.println("Exception in processFile "+e.getMessage());
					e.printStackTrace();
				}

			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (printWriter != null) {
				printWriter.close();
			}
		}

		System.out.println("Test phase is " + testPhase + " and Total count: "
				+ count);

	}

	private CPOutBoxItem parseLine(String line, String testPhase) {
		
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		String today = ft.format(dNow);

		CPOutBoxItem cpBoxItem = new CPOutBoxItem();
		String[] variables = line.split(",");
		if (variables.length > 0) {
				cpBoxItem.setLoy_id(variables[0]);
				cpBoxItem.setMd_tag(variables[1]);
			// Variable 2 need to be mapped
			if ("PRESET".equalsIgnoreCase(testPhase)) {
				// Member number,Existing Tag,Effective Date,Send Date,Sent flag
				String sendDT=getDate(Integer.parseInt(variables[3]));
				cpBoxItem.setSend_date(sendDT);
				cpBoxItem.setStatus(Integer.parseInt(variables[4]));

			} else if ("VERIFY".equalsIgnoreCase(testPhase)) {
				// Member number,Incoming tags,Send Date
				String sendDT=getDate(Integer.parseInt(variables[2]));
				cpBoxItem.setSend_date(sendDT);

			}
		}
		return cpBoxItem;
	}

	private String createJson(String lyl_id_no, List<String> tagList) {
		StringBuilder jsonBuilder = new StringBuilder();

		jsonBuilder.append("{\"lyl_id_no\":\"").append(lyl_id_no)
				.append("\",\"tags\":[");
		if (!tagList.isEmpty()) {
			boolean firstTag = true;
			for (String tag : tagList) {
				if (firstTag) {
					firstTag = false;
					jsonBuilder.append("\"").append(tag).append("\"");
				} else {
					jsonBuilder.append(",\"").append(tag).append("\"");
				}
			}
		}
		jsonBuilder.append("]}");

		return jsonBuilder.toString();
	}

	
	private String getDate(int numofdays)
	{
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, numofdays); 
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd"); 
		return s.format(new Date(cal.getTimeInMillis()));
	}
	
	
}
