package analytics.util.objects;


import analytics.util.JsonUtils;
import org.json.simple.JSONObject;

/**
 * Created by ewasser on 1/20/15.
 */
public class ClientCampaign {
    private String client;
    private String channel;
    private String startDate;
    private String endDate;
    private int maxCount;
    private int currentCount;
    private String type;
    private String tagId;

    public ClientCampaign(JSONObject clientCmp) {
        this.client = clientCmp.get("client").toString();
        this.channel = clientCmp.get("channel").toString();
        this.startDate = clientCmp.get("startDate").toString();
        this.endDate = clientCmp.get("endDate").toString();
        this.maxCount = Integer.valueOf(clientCmp.get("maxCount").toString());
        this.currentCount = Integer.valueOf(clientCmp.get("currentCount").toString());
        this.type = clientCmp.get("type").toString();
        this.tagId = clientCmp.get("tagId").toString();
    }

    public ClientCampaign(String clnt, String chnnl, String strtDt, String endDt, int mxCnt, int crrntCt, String typ, String tag) {
        this.client = clnt;
        this.channel = chnnl;
        this.startDate = strtDt;
        this.endDate = endDt;
        this.maxCount = mxCnt;
        this.currentCount = crrntCt;
        this.type = typ;
        this.tagId = tag;
    }

}
