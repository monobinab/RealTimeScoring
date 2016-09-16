package analytics.util.objects;

import java.util.List;

public class KeyWordModelCode {
	
	
	public String getKeyWord() {
		return keyWord;
	}
	public void setKeyWord(String keyWord) {
		this.keyWord = keyWord;
	}
	public List<String> getModelCodesList() {
		return modelCodesList;
	}
	public void setModelCodesList(List<String> modelCodesList) {
		this.modelCodesList = modelCodesList;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	private String keyWord;
	private List<String> modelCodesList;
	private String date;

}
