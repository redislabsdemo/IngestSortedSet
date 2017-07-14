package com.redislabs.ingest.sortedsets;

import redis.clients.jedis.Jedis;

/*
 * SortedSetPublisher inserts a message into a Sorted Set
 * and increments the counter that tracks new messages. 
 */
public class SortedSetPublisher
{
	
	public static String SORTEDSET_COUNT_SUFFIX = "count";
	
	// Redis connection
	RedisConnection conn = null;
	
	// Jedis object
	Jedis jedis = null;
		
	// name of the Sorted Set data structure 
	private String sortedSetName = null;	

	/*
	 * @param name: TimeSeriesPublisher constructor
	 */
	public SortedSetPublisher(String name) throws Exception{
		sortedSetName = name;
		conn = RedisConnection.getRedisConnection();
		jedis = conn.getJedis();		
	}
	
	/*
	 */
	public void publish(String message) throws Exception{
		// Get timestamp
		long count = jedis.incr(sortedSetName+":"+SORTEDSET_COUNT_SUFFIX);
		
		// Insert into sorted set
		jedis.zadd(sortedSetName, (double)count, message);		
	}
	
}