package analytics.util.dao;

import analytics.util.MongoNameConstants;
import analytics.util.objects.ClientCampaign;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ewasser on 1/20/15.
 * gets a list of client campaigns by client or all campaigns
 */
public class ClientCampaignDao extends AbstractDao {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(VariableDao.class);
    DBCollection clientCampaignCollection;

    public ClientCampaignDao(){
        super();
        clientCampaignCollection = db.getCollection("Variables");
    }

    public List<ClientCampaign> getClientCampaignList(String clnt) {
        List<ClientCampaign> clientCampaigns = new ArrayList<ClientCampaign>();
        BasicDBObject cntQuery = new BasicDBObject(MongoNameConstants.CC_client,clnt);
        DBCursor clientCmpCursor = clientCampaignCollection.find(cntQuery);
        if(clientCmpCursor == null) {
            LOGGER.warn("Client campaign query returned no results");
            return clientCampaigns;
        }

        for (DBObject cc : clientCmpCursor) {
            clientCampaigns.add(new ClientCampaign(
                    cc.get(MongoNameConstants.CC_client).toString().toUpperCase(),
                    cc.get(MongoNameConstants.CC_channel).toString(),
                    cc.get(MongoNameConstants.CC_startDate).toString(),
                    cc.get(MongoNameConstants.CC_endDate).toString(),
                    Integer.valueOf(cc.get(MongoNameConstants.CC_maxCount).toString()),
                    Integer.valueOf(cc.get(MongoNameConstants.CC_currentCount).toString()),
                    cc.get(MongoNameConstants.CC_tagType).toString(),
                    cc.get(MongoNameConstants.CC_tagId).toString()));
        }
        return clientCampaigns;

    }
    public List<ClientCampaign> getAllCampaignList() {
        List<ClientCampaign> clientCampaigns = new ArrayList<ClientCampaign>();
        DBCursor clientCmpCursor = clientCampaignCollection.find();
        for (DBObject cc : clientCmpCursor) {
            clientCampaigns.add(new ClientCampaign(
                    cc.get(MongoNameConstants.CC_client).toString().toUpperCase(),
                    cc.get(MongoNameConstants.CC_channel).toString(),
                    cc.get(MongoNameConstants.CC_startDate).toString(),
                    cc.get(MongoNameConstants.CC_endDate).toString(),
                    Integer.valueOf(cc.get(MongoNameConstants.CC_maxCount).toString()),
                    Integer.valueOf(cc.get(MongoNameConstants.CC_currentCount).toString()),
                    cc.get(MongoNameConstants.CC_tagType).toString(),
                    cc.get(MongoNameConstants.CC_tagId).toString()));
        }
        return clientCampaigns;

    }

}
