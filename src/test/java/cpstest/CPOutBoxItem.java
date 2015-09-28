package cpstest;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

public class CPOutBoxItem {
	int email_pkg_id;
	String loy_id, bu, sub_bu, md_tag, occasion_name;
	String added_datetime;
	Date sent_datetime;
	String send_date;
	int status;
	String cust_event_name, customer_id, sears_opt_in, kmart_opt_in,
			syw_opt_in;
    List<String> mdTagList=new ArrayList<String>();
	public List<String> getMdTagList() {
		return mdTagList;
	}

	public void setMdTagList(List<String> mdTagList) {
		this.mdTagList = mdTagList;
	}

	public int getEmail_pkg_id() {
		return email_pkg_id;
	}

	public void setEmail_pkg_id(int email_pkg_id) {
		this.email_pkg_id = email_pkg_id;
	}

	public String getLoy_id() {
		return loy_id;
	}

	public void setLoy_id(String loy_id) {
		this.loy_id = loy_id;
	}

	public String getBu() {
		return bu;
	}

	public void setBu(String bu) {
		this.bu = bu;
	}

	public String getSub_bu() {
		return sub_bu;
	}

	public void setSub_bu(String sub_bu) {
		this.sub_bu = sub_bu;
	}

	public String getMd_tag() {
		return md_tag;
	}

	public void setMd_tag(String md_tag) {
		this.md_tag = md_tag;
	}

	public String getOccasion_name() {
		return occasion_name;
	}

	public void setOccasion_name(String occasion_name) {
		this.occasion_name = occasion_name;
	}

	public String getAdded_datetime() {
		return added_datetime;
	}

	public void setAdded_datetime(String timestamp) {
		this.added_datetime = timestamp;
	}

	public String getSend_date() {
		return send_date;
	}

	public void setSend_date(String sendDT) {
		this.send_date = sendDT;
	}

	public Date getSent_datetime() {
		return sent_datetime;
	}

	public void setSent_datetime(Date sent_datetime) {
		this.sent_datetime = sent_datetime;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getCust_event_name() {
		return cust_event_name;
	}

	public void setCust_event_name(String cust_event_name) {
		this.cust_event_name = cust_event_name;
	}

	public String getCustomer_id() {
		return customer_id;
	}

	public void setCustomer_id(String customer_id) {
		this.customer_id = customer_id;
	}

	public String getSears_opt_in() {
		return sears_opt_in;
	}

	public void setSears_opt_in(String sears_opt_in) {
		this.sears_opt_in = sears_opt_in;
	}

	public String getKmart_opt_in() {
		return kmart_opt_in;
	}

	public void setKmart_opt_in(String kmart_opt_in) {
		this.kmart_opt_in = kmart_opt_in;
	}

	public String getSyw_opt_in() {
		return syw_opt_in;
	}

	public void setSyw_opt_in(String syw_opt_in) {
		this.syw_opt_in = syw_opt_in;
	}

}
