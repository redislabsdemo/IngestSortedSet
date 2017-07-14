package com.redislabs.ingest.sortedsets;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

/*
 * SortedSetFilter is a parent class that implements logic to learn about
 * new messages, reload messages from the time-series database.
 */
public class SortedSetFilter extends Thread
{
	// RedisConnection to query the database 
	protected RedisConnection conn = null;
	
	protected Jedis jedis = null;
	
	protected String name = "SortedSetSubscriber"; // default name
	
	protected String subscriberChannel = "defaultchannel"; //default name
	
	// Name of the Sorted Set
	protected String sortedSetName = null;
	
	// Channel to publish the availability of a new message
	protected String publisherChannel = null;
	
	// The key of the last message processed
	protected String lastMsgKey = null;
	
	// The key of the latest message count
	protected String currentMsgKey = null;

	// Count to store the last message processed
	protected volatile String lastMsgCount = null;
	
	// Time-series publisher for the next level 
	protected SortedSetPublisher sortedSetPublisher = null;
	
	public static String LAST_MESSAGE_COUNT_SUFFIX="lastmessage";
	
	/*
	 * @param name: name of the SortedSetFilter object
	 * @param subscriberChannel: name of the channel to listen to the availability of new messages
	 * @param publisherChannel: name of the channel to publish the availability of new messages
	 */
	public SortedSetFilter(String name, String subscriberChannel, String publisherChannel) throws Exception{
		this.name = name;
		this.subscriberChannel = subscriberChannel;
		this.sortedSetName = subscriberChannel;
		this.publisherChannel = publisherChannel;
		this.lastMsgKey = name+":"+LAST_MESSAGE_COUNT_SUFFIX;
		this.currentMsgKey = subscriberChannel+":"+SortedSetPublisher.SORTEDSET_COUNT_SUFFIX;
		
	}
	
	@Override
	public void run(){
		try{
			
			// Connection for reading/writing to sorted sets
			conn = RedisConnection.getRedisConnection();
			jedis = conn.getJedis();
			
			if(publisherChannel != null){
				sortedSetPublisher = new SortedSetPublisher(publisherChannel);
			}

			// load delta data since last connection
			while(true){
				fetchData();	
			}					
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	/*
	 * init() method loads the count of the last message processed. It then loads all messages 
	 * since the last count. 
	 */
	private void fetchData() throws Exception{
		if(lastMsgCount == null){
			lastMsgCount = jedis.get(lastMsgKey);
			if(lastMsgCount == null){
				lastMsgCount = "0";
			}
		}
		
		String currentCount = jedis.get(currentMsgKey); 
		
		if(currentCount != null && Long.parseLong(currentCount) > Long.parseLong(lastMsgCount)){
			loadSortedSet(lastMsgCount, currentCount);  	
		}else{
			Thread.sleep(1000); //sleep for a second if there's no data to fetch
		}
	}
				
	
	//Call to load the data from last Count to current Count
	private void loadSortedSet(String lastMsgCount, String currentCount) throws Exception{
		//Read from TimeSeries DB
		Set<Tuple> CountTuple = jedis.zrangeByScoreWithScores(sortedSetName, lastMsgCount, currentCount);
		
		for(Tuple t : CountTuple){
			processMessageTuple(t);
		}
		
	}
	
	// Override this method to customize the filters
	private void processMessageTuple(Tuple t) throws Exception{
		long score = new Double(t.getScore()).longValue();
		String message = t.getElement();
		lastMsgCount = (new Long(score)).toString();
		processMessage(message);
		
		jedis.set(lastMsgKey, lastMsgCount);
	}
	
	protected void processMessage(String message) throws Exception{
		//Override this method 
	}
}