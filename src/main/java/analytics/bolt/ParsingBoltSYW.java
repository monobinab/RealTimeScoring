package analytics.bolt;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

import static backtype.storm.utils.Utils.tuple;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import analytics.util.SYWAPICalls;
import analytics.util.Signing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ParsingBoltSYW extends BaseRichBolt {

	private OutputCollector outputCollector;
	private List<String> listOfInteractionsForRTS;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		listOfInteractionsForRTS = new ArrayList<String>();
		//Master list of interactions we process
		listOfInteractionsForRTS.add("Like");
	}

	@Override
	public void execute(Tuple input) {

		JsonParser parser = new JsonParser();
		JsonArray interactionArray = parser.parse(
				input.getStringByField("message")).getAsJsonArray();

		// Each record can have multiple elements in it, though it is generally
		// only one
		for (JsonElement interaction : interactionArray) {
			JsonObject interactionObject = interaction.getAsJsonObject();
			String l_id = SYWAPICalls.getLoyaltyId(interactionObject.get(
					"UserId").getAsString());
			if (l_id == null) {
				System.out
						.println("Unable to get member info, skipping record");
				continue;
			} else {
				l_id = Signing.hashLoyaltyId(l_id);
			}
			JsonElement interactionType = interactionObject
					.get("InteractionType");
			//Ignore interactions that we dont want
			if (listOfInteractionsForRTS
					.contains(interactionType.getAsString())) {
				outputCollector.emit(tuple(l_id, interactionObject,
						interactionType));
			} else {
				System.out.println("Ignoring " + interactionType);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "message", "InteractionType"));
		/*
		 * AddProductMedia AddToCatalog AddToPendingPoll Answer Ask
		 * CatalogCreated Comment CommentOnStory CreatePoll EarnBadge
		 * FilteredNewsfeed Follow FollowTag Import ImportedExternalProduct
		 * InviteFriends Like LikedCatalog LikeStory MessageWrittenOnWall
		 * MobileFirstUse Own Post PostedPhotoOnTagWall
		 * PostedTextMessageOnTagWall PostedVideoOnTagWall PostShopin
		 * ProductQuickview ProductWasAddedToCart ProductWasRecommended
		 * ProfileUpdated QuestionAsked Rate Recommend Review SearchFacetChosen
		 * Share SharedProduct SharedStory Shopin StatusUpdate Tagged TagItem
		 * UnfollowTag UploadedImage UserJoined UserNotInterestedIn View
		 * VisitTag VotedOnTopicalPoll VoteOnPoll Want
		 */
	}

}
