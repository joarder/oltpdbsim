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

public abstract class TwitterConstants {
	public static final String TBL_USER = "user_profiles";
    public static final String TBL_TWEETS = "tweets";
    public static final String TBL_FOLLOWS = "follows";
    public static final String TBL_FOLLOWERS = "followers";

    public static final int NUM_USERS = 500; // Twitter Scale Factor - Number of Users    
    public static final int NUM_TWEETS = 20000;    
    public static final int MAX_FOLLOW_PER_USER = 50;
    public static final int LIMIT_TWEETS = 100;
    public static final int LIMIT_TWEETS_FOR_UID = 10;
    public static final int LIMIT_FOLLOWERS = 20;
    
	public static final double ZIPF_EXP = 2.0; // 1.75
}