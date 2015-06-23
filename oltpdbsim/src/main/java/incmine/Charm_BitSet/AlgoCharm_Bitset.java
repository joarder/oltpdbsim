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
/**
 * Source: http://www.cs.waikato.ac.nz/~abifet/MOA-IncMine/
 */

package main.java.incmine.Charm_BitSet;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.*;

public class AlgoCharm_Bitset {
        protected Itemsets closedItemsets = new Itemsets();
        protected Context context;
        
	private long startTimestamp; // for stats
	private long endTimestamp; // for stats
	private int minsupRelative;

	Map<Integer, BitSet> mapItemTIDS = new HashMap<Integer, BitSet>();

	int tidcount;
	@SuppressWarnings("unused")
	private int itemsetCount;

	// for optimization with a hashTable
	private HashTable hash;

	public AlgoCharm_Bitset() {
	}

	/**
	 * This algorithm has two parameters
	 * 
	 * @param minsupp
	 *            the minimum support
	 * @param itemCount
	 * @return
	 * @throws IOException
	 */
	public Itemsets runAlgorithm(Context context, double minsup,
			int hashTableSize) {
		this.hash = new HashTable(hashTableSize);
		startTimestamp = System.currentTimeMillis();

		// (1) count the tid set of each item in the database in one database
		// pass
		mapItemTIDS = new HashMap<Integer, BitSet>(); // id item, count
		tidcount = 0;
		for(int i=0; i<context.size(); i++) { // for each transaction
			for (Integer item:context.getObjects().get(i).getItems()) {
				BitSet tids = mapItemTIDS.get(item);
				if (tids == null) {
					tids = new BitSet();
					mapItemTIDS.put(item, tids);
				}
				tids.set(tidcount);
			}
			tidcount++;
		}

		this.minsupRelative = (int) Math.ceil(minsup * tidcount);

		// (2) create ITSearchTree with root node
		ITSearchTree tree = new ITSearchTree();
		ITNode root = new ITNode(new HashSet<Integer>());
		root.setTidset(null, tidcount);
		tree.setRoot(root);

		// (3) create childs of the root node.
		for (Entry<Integer, BitSet> entry : mapItemTIDS.entrySet()) {
			int entryCardinality = entry.getValue().cardinality();
			// we only add nodes for items that are frequents
			if (entryCardinality >= minsupRelative) {
				// create the new node
				Set<Integer> itemset = new HashSet<Integer>();
				itemset.add(entry.getKey());
				ITNode newNode = new ITNode(itemset);
				newNode.setTidset(entry.getValue(), entryCardinality);
				newNode.setParent(root);
				// add the new node as child of the root node
				root.getChildNodes().add(newNode);
			}
		}

		// for optimization
		sortChildren(root);

		while (root.getChildNodes().size() > 0) {
			ITNode child = root.getChildNodes().get(0);
			extend(child);
			save(child);
			delete(child);
		}

		saveAllClosedItemsets();

		endTimestamp = System.currentTimeMillis();
                return closedItemsets;
	}
        
        public long getExecTime(){
            return endTimestamp - startTimestamp;
        }

	private void saveAllClosedItemsets() {
		for (List<Itemset> hashE : hash.table) {
			if (hashE != null) {
				for (Itemset itemsetObject : hashE) {
					closedItemsets.addItemset(itemsetObject);
					itemsetCount++;
				}
			}
		}
	}

	private void extend(ITNode currNode) {
		// loop over the brothers
		int i = 0;
		while (i < currNode.getParent().getChildNodes().size()) {

			ITNode brother = currNode.getParent().getChildNodes().get(i);
			if (brother != currNode) {

				// Property 1
				if (currNode.getTidset().equals(brother.getTidset())) {
					replaceInSubtree(currNode, brother.getItemset());
					delete(brother);
				}
				// Property 2
				else if (containsAll(brother, currNode)) {
					replaceInSubtree(currNode, brother.getItemset());
					i++;
				}
				// Property 3
				else if (containsAll(currNode, brother)) {
					ITNode candidate = getCandidate(currNode, brother);
					delete(brother);
					if (candidate != null) {
						currNode.getChildNodes().add(candidate);
						candidate.setParent(currNode);
					}
				}
				// Property 4
				else if (!currNode.getTidset().equals(brother.getTidset())) {
					ITNode candidate = getCandidate(currNode, brother);
					if (candidate != null) {
						currNode.getChildNodes().add(candidate);
						candidate.setParent(currNode);
					}
					i++;
				} else {
					i++;
				}
			} else {
				i++;
			}
		}

		sortChildren(currNode);

		while (currNode.getChildNodes().size() > 0) {
			ITNode child = currNode.getChildNodes().get(0);
			extend(child);
			save(child);
			delete(child);
		}
	}

	private boolean containsAll(ITNode node1, ITNode node2) {
		BitSet newbitset = (BitSet) node2.getTidset().clone();
		newbitset.and(node1.getTidset());
		return newbitset.cardinality() == node2.size();
	}

	private void replaceInSubtree(ITNode currNode, Set<Integer> itemset) {
		// make the union
		Set<Integer> union = new HashSet<Integer>(itemset);
		union.addAll(currNode.getItemset());
		// replace for this node
		currNode.setItemset(union);
		// replace for the childs of this node
		currNode.replaceInChildren(union);
	}

	private ITNode getCandidate(ITNode currNode, ITNode brother) {

		// create list of common tids.
		BitSet commonTids = (BitSet) currNode.getTidset().clone();
		commonTids.and(brother.getTidset());
		int cardinality = commonTids.cardinality();

		// (2) check if the two itemsets have enough common tids
		// if not, we don't need to generate a rule for them.
		if (cardinality >= minsupRelative) {
			Set<Integer> union = new HashSet<Integer>(brother.getItemset());
			union.addAll(currNode.getItemset());
			ITNode node = new ITNode(union);
			node.setTidset(commonTids, cardinality);
			return node;
		}

		return null;
	}

	private void delete(ITNode child) {
		child.getParent().getChildNodes().remove(child);
	}

	private void save(ITNode node) {
		if (!hash.containsSupersetOf(node.itemsetObject)) {
			hash.put(node.itemsetObject);
		}
	}

	private void sortChildren(ITNode node) {
		// sort children of the node according to the support.
		Collections.sort(node.getChildNodes(), new Comparator<ITNode>() {
			// Returns a negative integer, zero, or a positive integer as
			// the first argument is less than, equal to, or greater than the
			// second.
			public int compare(ITNode o1, ITNode o2) {
				return o1.getTidset().size() - o2.getTidset().size();
			}
		});
	}
}
