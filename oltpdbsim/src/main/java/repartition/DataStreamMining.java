package main.java.repartition;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.dsm.FCICluster;
import main.java.entry.Global;
import main.java.incmine.core.SemiFCI;
import main.java.incmine.learners.IncMine;
import main.java.incmine.streams.ZakiFileStream;
import main.java.utils.Utility;
import main.java.utils.graph.SimHypergraph;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleHypergraph;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;
import main.java.workload.WorkloadExecutor;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Sets;

public class DataStreamMining {

	public SimpleHypergraph<SimpleVertex, SimpleHEdge> hgr;
	public static HashMap<Integer, FCICluster> fci_clusters;
	Queue<String> dsm_queue;
	private IncMine dsm_learner;
	private ZakiFileStream dsm_stream;
	private PrintWriter dsm_PrintWriter;	
	private String dsm_dumpfilename;
	private File dsm_dumpfile;
	private int dsm_dump_serial;
	private int dsm_line_serial;
	private static int dsm_size;
	
	public DataStreamMining() {
		dsm_size = Global.observationWindow * Global.streamCollectorSizeFactor;
		
		this.dsm_dump_serial = 0;
		this.dsm_line_serial = 0;
		this.dsm_queue = EvictingQueue.create(dsm_size); // Circular FIFO
		this.dsm_learner = new IncMine();
		
		// Configure the learner
		this.dsm_learner.minSupportOption.setValue(0.1d); // 0.1 as the default value
		this.dsm_learner.relaxationRateOption.setValue(0.5d); // 0.5 as the default value
		this.dsm_learner.fixedSegmentLengthOption.setValue(Global.observationWindow); // 1000 is the default value 		
		this.dsm_learner.windowSizeOption.setValue(Global.streamCollectorSizeFactor); // 10 is the default value
		this.dsm_learner.maxItemsetLengthOption.setValue(-1); // - 1 means disabled, will perform full stream mining 
		this.dsm_learner.resetLearning();
	}
	
	public void collectStream(Cluster cluster, Transaction tr) {
		boolean isDumpFull = false;
		String stream = "";
		
		++this.dsm_line_serial;
		
		Iterator<Integer> data = tr.getTr_dataSet().iterator();			
		stream += this.dsm_line_serial+" "+this.dsm_line_serial+" "+tr.getTr_dataSet().size()+" ";
		
		while(data.hasNext()) {
			stream += cluster.getData(data.next()).getData_id();
			
			if(data.hasNext())
				stream += " ";
		}
		
		// Add the current stream in the queue
		this.dsm_queue.add(stream);
		
		if(this.dsm_line_serial == dsm_size) 
			isDumpFull = true;
		
		if(isDumpFull) {
			isDumpFull = false;
			this.dsm_line_serial = 0;
			
			// Get the latest streams in a file
			this.prepareDSMDumpFile();
		}
	}		
	
	// Prepares a dump file containing most recent transaction streams based on the queue size
	private void prepareDSMDumpFile() {
		++this.dsm_dump_serial;
		
		this.dsm_dumpfilename = Global.mining_dir+Global.simulation+"-dump-"+this.dsm_dump_serial+".txt";
		this.dsm_dumpfile = new File(this.dsm_dumpfilename);
		this.dsm_PrintWriter = Utility.getPrintWriter(Global.mining_dir, this.dsm_dumpfile);
		
		// Write in the dump file
		Iterator<String> itr = this.dsm_queue.iterator();
		while(itr.hasNext()) {
			this.dsm_PrintWriter.println(itr.next());
		}
		
		this.dsm_PrintWriter.flush();
		this.dsm_PrintWriter.close();
	}
	
	public void performDSM(Cluster cluster, WorkloadBatch wb) {		
		// Read the stream input
		this.dsm_stream = new ZakiFileStream(this.dsm_dumpfilename);
		this.dsm_stream.prepareForUse();				
        
		// Perform DSM
		while(dsm_stream.hasMoreInstances()){
			this.dsm_learner.trainOnInstance(this.dsm_stream.nextInstance());            
        }
		
		// Testing
		//System.out.println(this.dsm_learner);		
		Global.LOGGER.info("Total "+this.dsm_learner.getFCITable().size()+" frequent tuple sets have been identified.");
		
		this.performARHP(cluster, wb);
	} 
	
	// Association Rule Hypergraph Partitioning (ARHP)
	private void performARHP(Cluster cluster, WorkloadBatch wb) {
		// Create a hypergraph from the FCI list
		Global.LOGGER.info("Creating association rule hypergraph ...");
		
		hgr = new SimHypergraph<SimpleVertex, SimpleHEdge>();
		fci_clusters = new HashMap<Integer, FCICluster>();
		
		ArrayList<FCIHEdge> fciHEdgeList = new ArrayList<FCIHEdge>();
		HashMap<Integer, Integer> vertexMap = new HashMap<Integer, Integer>();
		int hEdgeId = 0;
		
		for(SemiFCI semiFCI : this.dsm_learner.getFCITable()){
			//System.out.println("@ "+semiFCI.getItems());
			
			HashSet<Integer> vertexSet = new HashSet<Integer>();
			
			if(semiFCI.getItems().size() > 1){					
				double hEdgeWeight = 0.0;
				
				for(int fci : semiFCI.getItems()) {
					vertexSet.add(fci);
					hEdgeWeight += semiFCI.getApproximateSupport();
				}
		
				fciHEdgeList.add(new FCIHEdge(++hEdgeId, (int)hEdgeWeight/semiFCI.getItems().size(), vertexSet));
				
			} else {				
				vertexMap.put(semiFCI.getItems().get(0), (int)semiFCI.getApproximateSupport()/Global.streamCollectorSizeFactor);				
			}
        } // end-for()
		
		// Creating actual hypergraph
		for(FCIHEdge h : fciHEdgeList) {
			// Updating vertex weight
			for(int v : h.vSet)
				h.vMap.put(v, vertexMap.get(v));
						
			hgr.addHEdge(new SimpleHEdge(h.id, h.weight), wb.getVertices(cluster, h.vMap));
		} // end-for()
		
		Global.LOGGER.info("Total "+hgr.getEdgeCount()+" transactions containing "
				+hgr.getVertexCount()+" data objects have identified for repartitioning.");
		
		// Workload file
		String wrl_file_name = Global.repartitioningCycle+"-"+Global.simulation; 
		String wrl_abs_file_name = Global.part_dir+Global.getRunDir()+wrl_file_name;						
		
		File workloadFile = new File(wrl_abs_file_name);
		
		wb.setWrl_file_name(wrl_abs_file_name);
		wb.setWrl_file(workloadFile);
		
		// Performing ARHP and data migration
		WorkloadBatchProcessor.generateHGraphWorkloadFile(cluster, wb, hgr);
		WorkloadExecutor.runRepartitioner(cluster, wb);		
		DataMigration.performDataMigration(cluster, wb);		
				
		// Testing
		/*for(SimpleVertex v : this.hgr.getVertices()) {
			Data data = cluster.getData(v.getId());
			System.out.println(">> V"+v.getId()+"|W="+v.getWeight()+"|S"+v.getSid()+"|P"+v.getPid()+"|Incident h="+wb.hgr.getIncidentEdges(v).size());
			System.out.println(data.toString());
		}*/
		
		// Preparing the clusters with their associated weights
		double maxClusterWeight = Double.MIN_VALUE;
		for(Entry<Integer, FCICluster> fci_cluster : fci_clusters.entrySet()) {
			// Get the FCI weight
			double clusterWeight = 0.0;
			for(int fci : fci_cluster.getValue().fci)
				clusterWeight += vertexMap.get(fci);			
			
			if(clusterWeight >= maxClusterWeight)
				maxClusterWeight = clusterWeight;
			
			fci_cluster.getValue().weight = clusterWeight;			
		}
		
		// Normalizing cluster Weights
		for(Entry<Integer, FCICluster> fci_cluster : fci_clusters.entrySet()) {
			fci_cluster.getValue().weight = fci_cluster.getValue().weight/maxClusterWeight;
			System.out.println(fci_cluster);
		}
				
		// Execution - Data migration
		DataMigration.strategyARHC(cluster, wb);
	}
	
	public static PriorityQueue<AssociativeTr> pq;
	public static HashMap<Integer, AssociativeTr> tMap;
	
	// Populates a priority queue to keep the potential transactions 
	public static void populatePQ(Cluster cluster, WorkloadBatch wb) {		
		
		if(Global.idt_priority == 1.0)		
			pq = new PriorityQueue<AssociativeTr>(wb.hgr.getEdges().size(), AssociativeTr.by_MAX_ASSOCIATION_IMPROVEMENT());
		else if((1 - Global.idt_priority) == 1.0)
			pq = new PriorityQueue<AssociativeTr>(wb.hgr.getEdges().size(), AssociativeTr.by_MAX_LB_IMPROVEMENT());
		else {
			if(Global.idt_priority > (1 - Global.idt_priority))
				pq = new PriorityQueue<AssociativeTr>(wb.hgr.getEdges().size(), AssociativeTr.by_MAX_ASSOCIATION_IMPROVEMENT());
			else
				pq = new PriorityQueue<AssociativeTr>(wb.hgr.getEdges().size(), AssociativeTr.by_MAX_LB_IMPROVEMENT());
		}
		
		tMap = new HashMap<Integer, AssociativeTr>();
				
		for(SimpleHEdge h : wb.hgr.getEdges()) {			
			AssociativeTr t = prepare(cluster, wb, h);
			tMap.put(t.id, t);
			
			if(!t.isProcessed && t.isAssociated)
				pq.add(tMap.get(t.id));
		}
	}		
	
	// Prepares current transaction for processing
	private static AssociativeTr prepare(Cluster cluster, WorkloadBatch wb, SimpleHEdge h) {
		Transaction tr = wb.getTransaction(h.getId());			
		AssociativeTr t = new AssociativeTr(tr.getTr_id(), tr.getTr_period());

		t.populateServerSet(cluster, tr);
				
		if(t.dataMap.size() > 1) {	 // DTs
			t.populateAssociationList(cluster, wb, fci_clusters);
			//System.out.println("-->"+t.associationMap);
		} else {					// non-DTs
			t.min_dmgr = 0;
			t.max_association_gain = 0.0;
			t.isProcessed = true;
		}
		
		return t;
	}
	
	private static boolean isContainsAll(AssociativeTr incidentT, MigrationPlan m) {
		
		boolean contains = false;
		
		for(int s_id : m.fromSet) {
			if(incidentT.dataMap.containsKey(s_id))
				if(incidentT.dataMap.get(s_id).containsAll(m.dataMap.get(s_id)))
					contains = true;
		}
		
		if(contains)
			return false;
		else
			return true;
	}	
	
	// Checks whether processing current transaction affect any other transaction adversely
	public static boolean isAffected(WorkloadBatch wb, AssociativeTr t, MigrationPlan m) {			
		// Search the incident transactions for the targeted data rows to be moved
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d);
				
				for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
					AssociativeTr incidentT = tMap.get(h.getId());				
					
					if(!incidentT.equals(t) && incidentT.isProcessed) {					
						if(incidentT.dataMap.containsKey(m.to)) { // Either no change or potential reduction in the impact  
							return false;
						} else { // Destination server is not covered by the incident transaction						
							if(!isContainsAll(incidentT, m)) // Either no change or potential reduction in the impact
								return false;
							else // Not all of the data ids from the source servers are included in the migration plan
								return true;
						}
					}
				}
			}
		}
		
		return false;		
	}
	
	public static void processTransaction(Cluster cluster, WorkloadBatch wb, AssociativeTr t, MigrationPlan m) {
		
		// Data migrations
		HashMap<Integer, HashSet<Integer>> dataMap = new HashMap<Integer, HashSet<Integer>>(m.dataMap);			
		dataMigration(cluster, m.to, dataMap);
		
		// Adjust transaction's serverSet				
		for(int s_id : m.fromSet) {
			for(int d : t.dataMap.get(s_id))
				t.dataMap.get(m.to).add(d);			
			
			t.dataMap.remove(s_id);
		}
		
		// Update incident transactions
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d);
				
				for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
					AssociativeTr incidentT = tMap.get(h.getId());
					//System.out.println("\t\t--> "+incidentT.toString());
					
					if(!incidentT.equals(t) && !incidentT.isProcessed) {				
						pq.remove(incidentT);
						tMap.remove(h.getId());
						
						AssociativeTr new_incidentT = prepare(cluster, wb, h);
						tMap.put(new_incidentT.id, new_incidentT);				
						
						 // Only DTs will be added back after recalculations
						if(new_incidentT.dataMap.size() > 1)				
							pq.add(tMap.get(new_incidentT.id));						
					}
				}
			}
		}
		
		t.isProcessed = true;
	} 
	
	// Perform data migrations
	private static void dataMigration(Cluster cluster, int dst_server_id, HashMap<Integer, HashSet<Integer>> dataMap) {
		// Chose the destination partition ids
		Server dst_server = cluster.getServer(dst_server_id);
		ArrayList<Partition> dst_partitionList = new ArrayList<Partition>();		
		
		for(int p : dst_server.getServer_partitions())
			dst_partitionList.add(cluster.getPartition(p));	

		Collections.sort(dst_partitionList, Partition.BY_DATA_SIZE());
		
		int dst_partition_id = 0;
		int r = 0;
		
		for(Entry<Integer, HashSet<Integer>> entry : dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				Data data = cluster.getData(d);
		
				if(dataMap.size() > 1) {				
					if(r == dst_partitionList.size())
						r = 0;
					
					dst_partition_id = dst_partitionList.get(r).getPartition_id();				
					++r;
					
				} else {
					dst_partition_id = dst_partitionList.get(0).getPartition_id();
				}
				
				DataMigration.migration(cluster, dst_server_id, dst_partition_id, data);		
			}
		}
	}
}

class AssociativeTr implements Comparable<AssociativeTr> {
	int id;
	int min_dmgr;
	double period;	
	double max_association_gain; // Higher is better
	double max_lb_gain; // Lower is better		
	
	HashMap<Integer, HashSet<Integer>> dataMap;
	HashMap<Integer, Double> associationMap;
	boolean isAssociated;
	
	List<MigrationPlan> migrationPlanList;	
	boolean isProcessed;
	
	AssociativeTr(int id, double period) {
		this.id = id;
		this.min_dmgr = Integer.MAX_VALUE;
		this.period = period;
		this.max_association_gain = 0;
		this.max_lb_gain = Integer.MAX_VALUE;
		this.dataMap = new HashMap<Integer, HashSet<Integer>>();
		this.associationMap = new HashMap<Integer, Double>();
		this.migrationPlanList = new ArrayList<MigrationPlan>();
		this.isAssociated = false;
		this.isProcessed = false;
	}

	void populateServerSet(Cluster cluster, Transaction tr) {
		for(int d_id : tr.getTr_dataSet()) {
			Data d = cluster.getData(d_id);			
			int s_id = d.getData_server_id();
			
			if(this.dataMap.containsKey(s_id)) {
				this.dataMap.get(s_id).add(d.getData_id());
			} else {
				HashSet<Integer> dataSet = new HashSet<Integer>();
				dataSet.add(d.getData_id());
				this.dataMap.put(s_id, dataSet);
			}
		}
	}
		
	@SuppressWarnings("rawtypes")
	static TreeSet associationRank;
	@SuppressWarnings("rawtypes")
	static TreeSet lbRank;
	
	// Calculate the similarity/association of each transaction and the derived clusters
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void populateAssociationList(Cluster cluster, WorkloadBatch wb, 
			HashMap<Integer, FCICluster> fci_clusters) {
		
		// Sets for preserving the ranks
		associationRank = new TreeSet();
		lbRank = new TreeSet();
		
		for(Entry<Integer, HashSet<Integer>> entry : this.dataMap.entrySet()) {
			HashMap<Integer, HashSet<Integer>> dataMap;
			HashSet<Integer> fromSet = new HashSet<Integer>();			
			int to = entry.getKey();
			int req_dmgr = 0;
			
			// Get the 'from' server set 
			for(int from : this.dataMap.keySet()) {
				if(from !=  to)
					fromSet.add(from);
			}
			
			dataMap = new HashMap<Integer, HashSet<Integer>>();
			// Get the tuple id from the 'from' server set
			for(int from : fromSet) {						
				req_dmgr += this.dataMap.get(from).size();
				dataMap.put(from, this.dataMap.get(from));
			}
			
			MigrationPlan m = new MigrationPlan(fromSet,to, dataMap, req_dmgr);
			this.migrationPlanList.add(m); // From Source Server
			
			if(Global.idt_priority == 1.0)
				m.association_gain_per_dmgr = getAssociationGain(fci_clusters, entry, req_dmgr);
			else if((1 - Global.idt_priority) == 1.0)
				m.lb_gain_per_dmgr = getLbGain(cluster, this, m);
			else {
				m.association_gain_per_dmgr = getAssociationGain(fci_clusters, entry, req_dmgr);						
				m.lb_gain_per_dmgr = getLbGain(cluster, this, m);						
			}
			
			associationRank.add(m.association_gain_per_dmgr);
			lbRank.add(m.lb_gain_per_dmgr);						
						
			if(m.association_gain_per_dmgr > 0)
				this.isAssociated = true;
			
		} //end-for()
		
		// Setting the maximum Association and Lb gains for this transaction
		this.max_association_gain = (double) associationRank.last();
		this.max_lb_gain = (double) lbRank.first();
		
		// Sorting
		if(Global.idt_priority == 1.0) {
			// Sorting in descending order by the Association
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {				
					return (((double)m1.association_gain_per_dmgr > (double)m2.association_gain_per_dmgr) ? -1 : 
						((double)m1.association_gain_per_dmgr < (double)m2.association_gain_per_dmgr) ? 1 : 0);				
				}
			});
			
			this.max_association_gain = this.migrationPlanList.get(0).association_gain_per_dmgr;			
						
			// Testing
			// After sorting
			/*System.out.println("-------------------------------------------------------------------------");
			System.out.println("Sorting based on association gain per data migration ...");
			System.out.println("--> "+this.toString());
			for(MigrationPlan m : this.migrationPlanList) {
				System.out.println("\t"+m.toString());
			}*/

		} else if((1 - Global.idt_priority) == 1.0) {
			// Sorting in ascending order by Lb
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {				
					return (((double)m2.lb_gain_per_dmgr > (double)m1.lb_gain_per_dmgr) ? -1 : 
						((double)m2.lb_gain_per_dmgr < (double)m1.lb_gain_per_dmgr) ? 1 : 0);				
				}
			});
			
			this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_dmgr;
			
			// Testing
			// After sorting
			/*System.out.println("-------------------------------------------------------------------------");
			System.out.println("Sorting based on load balance improvement per data migration ...");
			System.out.println("--> "+this.toString());
			for(MigrationPlan m : this.migrationPlanList) {
				System.out.println("\t"+m.toString());
			}*/
			
		} else {
			// Sort the array list by the value of combined rank (descending order)
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {
					int association_rank1 = ((TreeSet) associationRank).headSet(m1.association_gain_per_dmgr).size();
		            int association_rank2 = ((TreeSet) associationRank).headSet(m2.association_gain_per_dmgr).size();
		            
		            int lb_rank1 = ((TreeSet) lbRank).tailSet(m1.lb_gain_per_dmgr).size();
		            int lb_rank2 = ((TreeSet) lbRank).tailSet(m2.lb_gain_per_dmgr).size();
		            
		            m1.combined_rank = association_rank1*Global.idt_priority + lb_rank1*(1 - Global.idt_priority);
		            m2.combined_rank = association_rank2*Global.idt_priority + lb_rank2*(1 - Global.idt_priority);
		            
					return (((double)m1.combined_rank > (double)m2.combined_rank) ? -1 : 
						((double)m1.combined_rank < (double)m2.combined_rank) ? 1 : 0);				
				}
			});
			
			this.max_association_gain = this.migrationPlanList.get(0).association_gain_per_dmgr;
			this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_dmgr;
			
			// Testing
			/*System.out.println("-------------------------------------------------------------------------");
			System.out.println("Sorting based on combined ranking ...");
			System.out.println("--> "+this.toString());
			for(MigrationPlan m : this.migrationPlanList) {
				System.out.println("\t"+m.toString());
			}*/
		}
		
		this.min_dmgr = this.migrationPlanList.get(0).req_dmgr;
	}
	
	// Returns the association gain
	private double getAssociationGain(HashMap<Integer, FCICluster> fci_clusters, 
			Entry<Integer, HashSet<Integer>> entry, int req_dmgr) {
		
		double association = 0.0;
		
		if(fci_clusters.containsKey(entry.getKey())) {
			double C_i = fci_clusters.get(entry.getKey()).fci.size();
			double T_C_i = Sets.intersection(fci_clusters.get(entry.getKey()).fci, entry.getValue()).size();		
			association = (double) (fci_clusters.get(entry.getKey()).weight * (T_C_i/C_i));
			association /= req_dmgr; 
			this.associationMap.put(entry.getKey(), association);
		}
		
		return association;
	}
	
	// Returns the lb gain
	private double getLbGain(Cluster cluster, AssociativeTr t, MigrationPlan m) {		
		DescriptiveStatistics new_server_data = new DescriptiveStatistics();
		
		for(Server s : cluster.getServers()) {
			if(t.dataMap.containsKey(s.getServer_id())) {
				
				if(m.fromSet.contains(s.getServer_id())) {
					int data_count = s.getServer_total_data() - t.dataMap.get(s.getServer_id()).size();
					new_server_data.addValue(data_count);
					
				} else if(m.to == s.getServer_id()) {
					int data_count = s.getServer_total_data() + t.dataMap.get(s.getServer_id()).size();
					new_server_data.addValue(data_count);
				}
			}
		}

		return (new_server_data.getVariance()/m.req_dmgr);
	}
	
	// Descending order
	static Comparator<AssociativeTr> by_MAX_ASSOCIATION_IMPROVEMENT() {
		return new Comparator<AssociativeTr>() {
			@Override
			public int compare(AssociativeTr t1, AssociativeTr t2) {
				return ((t1.max_association_gain > t2.max_association_gain) ? -1 : 
					(t1.max_association_gain < t2.max_association_gain) ? 1 : 0);
			}			
		};
	}
	
	// Ascending order
	static Comparator<AssociativeTr> by_MAX_LB_IMPROVEMENT() {
		return new Comparator<AssociativeTr>() {
			@Override
			public int compare(AssociativeTr t1, AssociativeTr t2) {
				return ((t2.max_lb_gain > t1.max_lb_gain) ? -1 : 
					(t2.max_lb_gain < t1.max_lb_gain) ? 1 : 0);
			}			
		};
	}
	
	@Override
	public int compareTo(AssociativeTr t) {					
		return (((int)this.id > (int)t.id) ? -1 : 
			((int)this.id < (int)t.id) ? 1 : 0);
	}
	
	@Override
	public String toString() {
		return (">> T"+this.id+": Max association gain ("+this.max_association_gain+") "
				+ "| Max Lb gain ("+this.max_lb_gain+") "
					+ "| "+this.associationMap
						+ "| "+this.dataMap);
	}
}

//
class FCIHEdge{
	int id;
	int weight;
	HashSet<Integer> vSet;
	HashMap<Integer, Integer> vMap;
	
	public FCIHEdge(int id, int weight, HashSet<Integer> vertexSet) {
		this.id = id;
		this.weight = weight;
		this.vSet = new HashSet<Integer>(vertexSet);
		this.vMap = new HashMap<Integer, Integer>();
	}
}