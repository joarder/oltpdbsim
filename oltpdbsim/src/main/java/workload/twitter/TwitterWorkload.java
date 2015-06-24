/*******************************************************************************
 * Copyright [2014] [Joarder Kamal]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
 
package main.java.workload.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import main.java.db.Database;
import main.java.db.Table;
import main.java.db.Tuple;
import main.java.entry.Global;
import main.java.utils.distributions.ScrambledZipfianGenerator;
import main.java.workload.Workload;
import main.java.workload.WorkloadConstants;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class TwitterWorkload extends Workload {	
	
	public TwitterWorkload(String file) {	
		super(file);				
	}

	// Read Twitter configuration file
	public void readConfig() {
		BufferedReader config_file = null; 
	    AbstractFileConfiguration parameters = null;
	    int i, j = 0;
	    
	    try {
	    	config_file = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(this.getFile_name())));
	    	
		//Load configuration parameters
	    	parameters = new PropertiesConfiguration();
			parameters.load(config_file);
			
		//Read Twitter scale
			WorkloadConstants.SCALE_FACTOR = parameters.getDouble("twitter.scale");		    	
			Global.LOGGER.info("Twitter scale (as the number of users): "+WorkloadConstants.SCALE_FACTOR);	
			
		//Read Twitter table types
			i = 1;
			for(Object param : parameters.getList("twitter.tbl.type")) {
				tbl_types.put(i, Integer.parseInt((String) param));				
				++i;
			}
			
			Global.LOGGER.info("Twitter tables types: "+tbl_types);
			
		//Read Twitter schema
			i = j = 0;
			ArrayList<Integer> temp = null;
			for(Object param : parameters.getList("twitter.schema")) {				
				if(j >= tbl_types.size() || j == 0){
					++i;
					j = 0;
					temp = new ArrayList<Integer>();
					schema.put(i, temp);					
				}
				
				schema.get(i).add(Integer.parseInt((String)param));
				++j;
			}
			
			Global.LOGGER.info("Twitter table-level schema: "+schema);
		
		//Read Twitter transaction proportion
			i = 0;
			//new 
			this.trTypes = new int[tr_types];
			this.trProbabilities = new double[tr_types];
			
			for(Object param : parameters.getList("twitter.trs.proportions")) {
				++i;
				tr_proportions.put(i, Double.parseDouble((String) param));
				
				this.trTypes[i-1] = i;
				this.trProbabilities[i-1] = Double.parseDouble((String) param);
			}
			
			Global.LOGGER.info("Twitter transaction proportions: "+tr_proportions);
			
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} finally {
			if(config_file != null) {
				try {
					config_file.close();			
				} catch (IOException e) {
					e.printStackTrace();
				}			
			}
		}	 
	}	
			
	// Creates a Tuple set for a specific transaction type
	@Override
	public Set<Integer> getTrTupleSet(Database db, int tr_type) {		
		//System.out.println("--> Generating a transaction of type "+tr_type+" ...");
		switch(tr_type) {
			case 1:
				return this.getFollowers(db, this.getUserId(db));					
				
			case 2:
				return this.getTweet(db, this.getTweetId(db));
				
			case 3:
				return this.getTweetsFromFollowing(db, this.getUserId(db));
				
			case 4:
				return this.getUserTweets(db, this.getUserId(db));
				
			case 5:
				return this.insertTweet(db);
		}		
		
		return null;		
	}
	
	// Returns a randomly selected User id
	private int getUserId(Database db) {
		Table tbl_user = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_USER));
		
		ScrambledZipfianGenerator szGen = new ScrambledZipfianGenerator(tbl_user.getTbl_tuples().size());
		int rand_tweet = szGen.nextInt();
		
		return rand_tweet;
	}
	
	// Returns a randomly selected Tweet id
	private int getTweetId(Database db) {
		Table tbl_tweets = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_TWEETS));			
		
		int rand_user = this.getUserId(db);		
		Set<Integer> user_tweets = tbl_tweets.getIdx_multivalue_secondary().get(rand_user);
		
		ArrayList<Integer> tweets = new ArrayList<Integer>(user_tweets);
		int rand_tweet = tweets.get(Global.rand.nextInt(tweets.size())); 
		
		return rand_tweet;
	}
	
	// Select Users that follow a given User (i.e. a 'followee') (0.07% of the Twitter workload)
	private Set<Integer> getFollowers(Database db, int followee) {
		Set<Integer> trTupleSet = new HashSet<Integer>();

		// Followers Table
		Table tbl = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_FOLLOWERS));
		Table tbl_user = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_USER));
		
		// Get the 'followers' for this 'followee'
		Set<Integer> followers = new HashSet<Integer>();
		
		if(tbl.getIdx_multivalue_secondary().containsKey(followee)) {
			for(int follower : tbl.getIdx_multivalue_secondary().get(followee)) {
				if(followers.size() <= TwitterConstants.LIMIT_FOLLOWERS) {
					followers.add(follower);				
					trTupleSet.add(db.getTupleByPk(tbl_user.getTbl_id(), follower).getTuple_id());		
				}
			}
		} else {
			// This User doesn't have any follower
		}
		
		return trTupleSet;
	}
	
	// Select a single Tweet (0.07% of the Twitter workload)
	private Set<Integer> getTweet(Database db, int tweet_id) {
		Set<Integer> trTupleSet = new HashSet<Integer>();

		Table tbl = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_TWEETS));
		Tuple tpl = db.getTupleByPk(tbl.getTbl_id(), tweet_id);
		
		trTupleSet.add(tpl.getTuple_id());	
		
		return trTupleSet;
	}
	
	// Select Tweets from Users that one (i.e. a given 'follower') follows (7.6725% of the Twitter workload)
	private Set<Integer> getTweetsFromFollowing(Database db, int follower) {
		Set<Integer> trTupleSet = new HashSet<Integer>();

		// Follows Table
		Table tbl = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_FOLLOWS));
		
		// Get the 'followees' for this 'follower'
		// Then find all 'tweets' for all of the retrieved 'followees'
		Set<Integer> followees = new HashSet<Integer>();
		Set<Integer> tweets = new HashSet<Integer>();
		
		for(int followee : tbl.getIdx_multivalue_secondary().get(follower)) {
			if(followees.size() <= TwitterConstants.MAX_FOLLOW_PER_USER) {
				followees.add(followee);
				
				trTupleSet.add(db.getTupleByPk(tbl.getTbl_id(), followee).getTuple_id());

				if(tweets.size() <= TwitterConstants.LIMIT_TWEETS)
					tweets.addAll(this.getUserTweets(db, followee));		
			}
		}
		
		// Adding the tweets in the tuple set
		trTupleSet.addAll(tweets);
		
		return trTupleSet;
	}	
	
	// Select Tweets for the given User id (91.2656% of the Twitter workload)
	private Set<Integer> getUserTweets(Database db, int user_id) {
		Set<Integer> trTupleSet = new HashSet<Integer>();
		Table tbl = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_TWEETS));

		int tuple_id = 0;
		for(int tweet_id : tbl.getIdx_multivalue_secondary().get(user_id)) {
			if(trTupleSet.size() <= TwitterConstants.LIMIT_TWEETS_FOR_UID) {
				tuple_id = db.getTupleByPk(tbl.getTbl_id(), tweet_id).getTuple_id();
				trTupleSet.add(tuple_id);
			}
		}
		
		return trTupleSet;
	}
	
	// Insert a single Tweet (0.9219% of the Twitter workload)
	private Set<Integer> insertTweet(Database db) {
		Set<Integer> trTupleSet = new HashSet<Integer>();
		
		Table tbl = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_ADDED_TWEETS));
		Table ftbl = db.getTable(db.getDb_tbl_name_id_map().get(TwitterConstants.TBL_USER));
		
		++Global.global_tupleSeq;
		int pk = tbl.getTbl_last_entry() + 1;
		Tuple tpl = db.insertTuple(tbl.getTbl_id(), pk);
		
		ScrambledZipfianGenerator szGen = new ScrambledZipfianGenerator(ftbl.getTbl_tuples().size());
		int fk = szGen.nextInt();
		
		// Populating foreign table relationships			
		if(tpl.getTuple_fk().containsKey(ftbl.getTbl_id())) {
			tpl.getTuple_fk().get(ftbl.getTbl_id()).add(fk);
			
		} else {
			Set<Integer> fkList = new HashSet<Integer>();
			fkList.add(fk);
			tpl.getTuple_fk().put(ftbl.getTbl_id(), fkList);
		}
		
		// Populating into index
		tbl.insertSecondaryIdx(fk, pk);	
	
		// Mark the last entry
		tbl.setTbl_last_entry(pk);
		
		// Insert into the Database
		tpl.setTuple_action(WorkloadConstants.TPL_INSERT);		
		
		// Adding tuples to the tuple set
		trTupleSet.add(tpl.getTuple_id());
		
		return trTupleSet;
	}
}