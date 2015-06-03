package main.java.repartition;

import java.util.HashMap;
import java.util.HashSet;

public class MigrationPlan {
	public HashSet<Integer> fromSet;	// From Server's Ids (for 1, 2, 3,  ..., (N-1) span reductions)
	public int to;		// To Server Id
	public int req_dmgr;
	public double idt_gain_per_dmgr;
	public double association_gain_per_dmgr;
	public double lb_gain_per_dmgr;
	public HashMap<Integer, HashSet<Integer>> dataMap;	
	public double combined_rank;
	
	public MigrationPlan(HashSet<Integer> fromSet, int to, HashMap<Integer, HashSet<Integer>> dataMap, int req_dmv) {
		this.fromSet = new HashSet<Integer>(fromSet);
		this.to = to;
		this.req_dmgr = req_dmv;
		this.idt_gain_per_dmgr = 0.0;
		this.lb_gain_per_dmgr = 0.0;
		this.dataMap = new HashMap<Integer, HashSet<Integer>>(dataMap);
		this.combined_rank = 0.0;
	}	
	
	@Override
	public String toString() {
		return ("-- From("+this.fromSet+") | To("+this.to+") | Required DMs("+this.req_dmgr+") | Idt improvement("+this.idt_gain_per_dmgr+") | Lb improvement ("+this.lb_gain_per_dmgr+") | Combined Rank ("+this.combined_rank+")");
	}
}