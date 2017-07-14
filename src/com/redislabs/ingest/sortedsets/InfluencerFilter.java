package com.redislabs.ingest.sortedsets;

import java.util.HashMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/*
 * This is a custom TimeSeriesFilter class that will enable filtering 
 * tweets of users with over 10,000 followers.
 * 
 */
public class InfluencerFilter extends SortedSetFilter
{
	/*
	 * @param name: name of the TimeSeriesFilter object
	 * @param subscriberChannel: name of the channel to listen to the availability of new messages
	 * @param publisherChannel: name of the channel to publish the availability of new messages
	 */
	public InfluencerFilter(String name, String subscriberChannel, String publisherChannel) throws Exception{
		super(name, subscriberChannel, publisherChannel);
	}
	
	/*
	 * @see com.redislabs.ingest.sortedsets.TimeSeriesFilter#processMessage(java.lang.String)
	 */
	@Override
	protected void processMessage(String message) throws Exception{
		JsonParser jsonParser = new JsonParser();
				
		JsonElement jsonElement = jsonParser.parse(message);
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		
		JsonObject userObject = jsonObject.get("user").getAsJsonObject();
		
		JsonElement followerCountElm = userObject.get("followers_count");
		try{
			if(followerCountElm != null && followerCountElm.getAsDouble() > 10000){
				String name = userObject.get("name").getAsString();
				String screenName = userObject.get("screen_name").getAsString();
				int followerCount = userObject.get("followers_count").getAsInt();
				int friendCount = userObject.get("friends_count").getAsInt();
							
				HashMap<String, String> map = new HashMap<String, String>();
				map.put("name", name);
				map.put("screen_name", screenName);
				if(userObject.get("location") != null){
					String loc = "";
					try{
						loc = userObject.get("location").getAsString();
					}catch(Exception e){
						loc = "";
					}
					map.put("location", loc);				
				}
				map.put("followers_count", Integer.toString(followerCount));
				map.put("friendCount", Integer.toString(friendCount));
				
				if(jedis != null){
					
					// Update Influencers list in the Sorted Set
					jedis.zadd("influencers", followerCount, screenName);
					
					// Update Hash
					jedis.hmset("influencer:"+screenName, map);
					System.out.println(userObject.get("screen_name").getAsString()+"| Followers:"+userObject.get("followers_count").getAsString());								
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/*
	 * Main method to start InfluencerFilter
	 */
	public static void main(String[] args) throws Exception{
		InfluencerFilter influencerFilter = new InfluencerFilter("InfluencerFilter", "alldata", null);
		influencerFilter.start();
	}
	

}