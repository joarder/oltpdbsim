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

import main.java.db.Database;
import main.java.db.Table;
import main.java.entry.Global;
import main.java.workload.twitter.TwitterConstants;
import main.java.workload.twitter.TwitterWorkload;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class TwitterDatabase extends Database {

	public TwitterDatabase(String name) {
		super(name);
	}
	
	public void estimateTableSize(TwitterWorkload twitter) {
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Estimating initial data tuple counts for "+this.getDb_name()+" database ...");
		
		int dependent = 0;
		
		for(int tbl_id = 1; tbl_id <= twitter.tbl; tbl_id++) {
			Table tbl = new Table(tbl_id, twitter.tbl_types.get(tbl_id), null);			
			
			if(tbl.getTbl_id() == 2) {
				// TBL_FOLLOWERS
				tbl.setTbl_data_rank(new int[TwitterConstants.NUM_USERS + 1]);				
				tbl.zipfDistribution = new ZipfDistribution(TwitterConstants.NUM_USERS,1.75);
				tbl.zipfDistribution.reseedRandomGenerator(Global.repeated_runs);			
				
			} else if(tbl.getTbl_id() == 3) {
				// TBL_FOLLOWS
				tbl.setTbl_data_rank(new int[TwitterConstants.NUM_USERS + 1]);
				tbl.zipfDistribution = new ZipfDistribution(TwitterConstants.MAX_FOLLOW_PER_USER, 1.75);
				tbl.zipfDistribution.reseedRandomGenerator(Global.repeated_runs);
			}
			
			this.getDb_tables().put(tbl_id, tbl);			
			
			// Determine the number of Data rows to be populated for each individual table
			switch(tbl.getTbl_name()) {
				case TwitterConstants.TBL_USER:
					tbl.setTbl_init_tuples((int)Math.round(TwitterConstants.NUM_USERS * twitter.scale));
					break;
				
				case TwitterConstants.TBL_FOLLOWERS:
					tbl.setTbl_init_tuples(0);
					break;
					
				case TwitterConstants.TBL_FOLLOWS:
					tbl.setTbl_init_tuples((int)Math.round(TwitterConstants.MAX_FOLLOW_PER_USER * twitter.scale));
					break;
					
				case TwitterConstants.TBL_TWEETS:
					tbl.setTbl_init_tuples((int)Math.round(TwitterConstants.NUM_TWEETS * twitter.scale));
					break;
					
				case TwitterConstants.TBL_ADDED_TWEETS:
					tbl.setTbl_init_tuples(0);
					break;	
			}
			
			Global.LOGGER.info(tbl.getTbl_name()+": "+tbl.getTbl_init_tuples());
			
			// Setting dependency information for the database tables
			if(tbl.getTbl_type() != 0){
				for(int i = 1; i <= twitter.tbl; i++) {
					dependent = twitter.schema.get(tbl.getTbl_id()).get(i-1); // ArrayList index starts from 0					
					
					if(dependent == 1)
						tbl.getTbl_foreign_tables().add(i);				
				}
			}
		}		
	}
}