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

package main.java.repartition;

import java.util.HashMap;
import java.util.HashSet;

public class MigrationPlan {
	public HashSet<Integer> fromSet;	// From Server's Ids (for 1, 2, 3,  ..., (N-1) span reductions)
	public int to;		// To Server Id
	public int req_data_mgr;
	
	public double span_reduction_per_data_mgr;
	public double delta_idt;
	public double delta_lb;
	public double associativity;	
	
	public double combined_weight;	
	public HashMap<Integer, HashSet<Integer>> serverDataSet;	
	
	public MigrationPlan(HashSet<Integer> fromSet, int to, HashMap<Integer, HashSet<Integer>> dataMap, int req_data_mgr) {
		this.fromSet = new HashSet<Integer>(fromSet);
		this.to = to;
		this.req_data_mgr = req_data_mgr;
		
		this.span_reduction_per_data_mgr = 0.0;
		this.delta_idt = 0.0;
		this.delta_lb = 0.0;
		this.associativity = 0.0;		
		
		this.combined_weight = 0.0;		
		this.serverDataSet = new HashMap<Integer, HashSet<Integer>>(dataMap);		
	}	
	
	@Override
	public String toString() {
		return ("-- From("+this.fromSet+") | To("+this.to+") "
				+ "| Combined Weight ("+this.combined_weight+") "
				+ "| Required data migrations ("+this.req_data_mgr+") "
					+ "| Span reduction gain ("+this.span_reduction_per_data_mgr+") "
						+ "| Idt gain ("+this.delta_idt+") "								
							+ "| Lb gain ("+this.delta_lb+") "
								+ "| Association gain ("+this.associativity+")");
	}
}