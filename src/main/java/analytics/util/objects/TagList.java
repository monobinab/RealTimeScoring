/**
 * 
 */
package analytics.util.objects;

import com.mongodb.BasicDBList;

/**
 * @author spannal
 *
 */
public class TagList {
	
	BasicDBList tags;
	BasicDBList rtsTags;
	/**
	 * @return the tags
	 */
	public BasicDBList getTags() {
		return tags;
	}
	/**
	 * @param tags the tags to set
	 */
	public void setTags(BasicDBList tags) {
		this.tags = tags;
	}
	/**
	 * @return the rtsTags
	 */
	public BasicDBList getRtsTags() {
		return rtsTags;
	}
	/**
	 * @param rtsTags the rtsTags to set
	 */
	public void setRtsTags(BasicDBList rtsTags) {
		this.rtsTags = rtsTags;
	}

}
