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

package main.java.db.twitter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import main.java.db.Database;
import main.java.db.Table;
import main.java.db.Tuple;
import main.java.entry.Global;
import main.java.utils.distributions.ScrambledZipfianGenerator;
import main.java.utils.distributions.ZipfianGenerator;
import main.java.workload.Workload;
import main.java.workload.WorkloadConstants;
import main.java.workload.twitter.TwitterConstants;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class TwitterDatabase extends Database {

	private int num_users;
	private int num_tweets;
	private int num_follows;
	
	public TwitterDatabase(String name) {
		super(name);
		
		this.num_users = 0;
		this.num_tweets = 0;
		this.num_follows = 0;
	}
	
	@Override
	public void populate(Workload wrl) {
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Estimating initial data tuple counts for '"+this.getDb_name()+"' database ...");
		
		String[] table_names = {
				TwitterConstants.TBL_USER,
				TwitterConstants.TBL_FOLLOWERS,
				TwitterConstants.TBL_FOLLOWS,
				TwitterConstants.TBL_TWEETS
		};
		
		this.num_users = (int)Math.round(TwitterConstants.NUM_USERS * WorkloadConstants.SCALE_FACTOR);
		this.num_tweets = (int)Math.round(TwitterConstants.NUM_TWEETS * WorkloadConstants.SCALE_FACTOR);
		this.num_follows = (int)Math.round(TwitterConstants.MAX_FOLLOW_PER_USER * WorkloadConstants.SCALE_FACTOR);
		
		int dependent = 0;
		
		for(int tbl_id = 1; tbl_id <= wrl.tbl; tbl_id++) {
			Table tbl = new Table(tbl_id, wrl.tbl_types.get(tbl_id), table_names[tbl_id-1]);
			
			this.getDb_tbl_name_id_map().put(tbl.getTbl_name(), tbl.getTbl_id());			
			this.getDb_tables().put(tbl_id, tbl);			
			
			// Setting dependency information for the database tables
			if(tbl.getTbl_type() != 0){
				for(int i = 1; i <= wrl.tbl; i++) {
					dependent = wrl.schema.get(tbl.getTbl_id()).get(i-1); // ArrayList index starts from 0					
					
					if(dependent == 1)
						tbl.getTbl_foreign_tables().add(i);				
				}
			}
		}
					
		// Populate initial database
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Populating initial database ...");
		
		// Loading Tables
		this.loadUsers();		
		this.loadFollowData();
		this.loadTweets();
		
		// Update tuple counts after initial population
		this.updateTupleCounts();
	}
	
	// Load Users	
	private void loadUsers() {		
		
		Table tbl = this.getTable(this.getDb_tbl_name_id_map().get(TwitterConstants.TBL_USER));
		
		tbl.zipfDistribution = new ZipfDistribution(this.num_users, TwitterConstants.ZIPF_EXP);
		tbl.zipfDistribution.reseedRandomGenerator(Global.repeatedRuns);
		
		int pk = 0;
		for (pk = 1; pk <= this.num_users; pk++) {
			++Global.global_tupleSeq;								
			this.insertTuple(tbl.getTbl_id(), pk);
		}
		
		tbl.setTbl_last_entry(pk);
		Global.LOGGER.info(tbl.getTbl_tuples().size()+" tuples have successfully inserted into '"+tbl.getTbl_name()+"' table.");
	}	
	
	/**
	 * The number of Tweets is fixed to num_tweets
	 * We simply select using the distribution who issued the Tweet
	 */		
	private void loadTweets() {		
		
		Table tbl = this.getTable(this.getDb_tbl_name_id_map().get(TwitterConstants.TBL_TWEETS));
		
		tbl.zipfDistribution = new ZipfDistribution(this.num_tweets, TwitterConstants.ZIPF_EXP);
		tbl.zipfDistribution.reseedRandomGenerator(Global.repeatedRuns);
		
		// Foreign Table
		Table ftbl = this.getTable(this.getDb_tbl_name_id_map().get(TwitterConstants.TBL_USER));
		
		ScrambledZipfianGenerator szGen = new ScrambledZipfianGenerator(this.num_users);
		
		int pk = 0, fk = 0;
		for (pk = 1; pk <= this.num_tweets; pk++) {
			fk = szGen.nextInt() + 1;
			//fk = ftbl.zipfDistribution.sample();
        	
			++Global.global_tupleSeq;											
			Tuple tpl = this.insertTuple(tbl.getTbl_id(), pk);
			
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
		}
		
		tbl.setTbl_last_entry(pk);
		Global.LOGGER.info(tbl.getTbl_tuples().size()+" tuples have successfully inserted into '"+tbl.getTbl_name()+"' table.");
	}
	
	/**
	 * For each User (Follower) we select how many users he is following (followees List)
     * then select users to fill up that list.
     * Selecting is based on the distribution.
     * NOTE: We are using two different distribution to avoid correlation:
     * ZipfianGenerator (describes the followed most) 
     * ScrambledZipfianGenerator (describes the heavy tweeters)
	 */
	private void loadFollowData() {		
		
		Table tbl_followers = this.getTable(this.getDb_tbl_name_id_map().get(TwitterConstants.TBL_FOLLOWERS));
		Table tbl_follows = this.getTable(this.getDb_tbl_name_id_map().get(TwitterConstants.TBL_FOLLOWS));

		// Foreign Tables
		ArrayList<Integer> ftblList_followers = new ArrayList<Integer>(tbl_followers.getTbl_foreign_tables());
		Table ftbl_followers = this.getTable(ftblList_followers.get(0));
		
		ArrayList<Integer> ftblList_follows = new ArrayList<Integer>(tbl_follows.getTbl_foreign_tables());
		Table ftbl_follows = this.getTable(ftblList_follows.get(0));		
				
		// Distributions
		tbl_followers.zipfDistribution = new ZipfDistribution(this.num_users, TwitterConstants.ZIPF_EXP);
		tbl_followers.zipfDistribution.reseedRandomGenerator(Global.repeatedRuns);
		
		tbl_follows.zipfDistribution = new ZipfDistribution(this.num_follows, TwitterConstants.ZIPF_EXP);
		tbl_follows.zipfDistribution.reseedRandomGenerator(Global.repeatedRuns);
		
		ZipfianGenerator zipfFollowee = new ZipfianGenerator(this.num_users, TwitterConstants.ZIPF_EXP);
		ZipfianGenerator zipfFollows = new ZipfianGenerator(this.num_follows, TwitterConstants.ZIPF_EXP);
		
		Set<Integer> allFollowees = new HashSet<Integer>();
        Set<Integer> followees = new HashSet<Integer>();        
        int follower = 0, followee = 0;
        
        for (follower = 1; follower <= this.num_users; follower++) {        	
        	// Adding a new 'Follower'
        	++Global.global_tupleSeq;			
        	Tuple follower_tpl = this.insertTuple(tbl_follows.getTbl_id(), follower);
        	
            followees.clear();

            int time = zipfFollows.nextInt() + 1;            
            //time = tbl_follows.zipfDistribution.sample();
            
            // At least this follower will follow 1 user
            if(time == 0) 
            	time = 1;
            
            for (int f = 0; f < time; ) {
                followee = zipfFollowee.nextInt() + 1;
            	//followee = tbl_followers.zipfDistribution.sample();
                
                if (follower != followee && !followees.contains(followee)) {
                	followees.add(followee);
                	
                	// Adding a new 'Followee' if not exists
                	Tuple followee_tpl;
                	
                	if(!allFollowees.contains(followee)) {
                		++Global.global_tupleSeq;			
                		followee_tpl = this.insertTuple(tbl_followers.getTbl_id(), followee);
                		
                	} else {
                		followee_tpl = tbl_followers.getTupleByPk(followee);
                	}
                	
                // Follows Table ('follower' follows 'followee' || f2 follows f1 )            				
    				// Populating foreign table relationships
    				if(follower_tpl.getTuple_fk().containsKey(ftbl_follows.getTbl_id())) {
    					follower_tpl.getTuple_fk().get(ftbl_follows.getTbl_id()).add(followee);
    					
    				} else {
    					Set<Integer> followeeSet = new HashSet<Integer>();
    					followeeSet.add(followee);
    					follower_tpl.getTuple_fk().put(ftbl_follows.getTbl_id(), followeeSet);
    				}
    				
    				// Insert into index
    				tbl_follows.insertSecondaryIdx(follower, followee);
                	
                // Followers Table ('followee' is followed by 'follower' || f1 is followed by f2)    				
    				// Populating foreign table relationships    				
    				if(followee_tpl.getTuple_fk().containsKey(ftbl_followers.getTbl_id())) {
    					followee_tpl.getTuple_fk().get(ftbl_followers.getTbl_id()).add(follower);
    					
    				} else {
    					Set<Integer> followerSet = new HashSet<Integer>();
    					followerSet.add(follower);
    					followee_tpl.getTuple_fk().put(ftbl_followers.getTbl_id(), followerSet);
    				}
    				
    				// Insert into index
    				tbl_followers.insertSecondaryIdx(followee, follower);
                                                            
                    ++f;
                }//end-if()
            }//end-for()
        }//end-for()
        
        tbl_followers.setTbl_last_entry(followee);
        tbl_follows.setTbl_last_entry(follower);
        
		Global.LOGGER.info(tbl_followers.getTbl_tuples().size()+" tuples have successfully inserted into '"+tbl_followers.getTbl_name()+"' table.");
		Global.LOGGER.info(tbl_follows.getTbl_tuples().size()+" tuples have successfully inserted into '"+tbl_follows.getTbl_name()+"' table.");
	}
}