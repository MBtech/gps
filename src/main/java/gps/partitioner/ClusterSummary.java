package gps.partitioner;

import java.util.ArrayList;
import java.util.LinkedList;

public class ClusterSummary {

	public final int[] clusterMap;
	public final int[] clusterNodeSizes;
	public final int[] clusterEdgeSizes;
	public LinkedList<ArrayList<Integer>> clusters = new LinkedList<ArrayList<Integer>>();

	public ClusterSummary(int[] clusterMap, int[] clusterNodeSizes, int[] clusterEdgeSizes) {
		this.clusterMap = clusterMap;
		this.clusterNodeSizes = clusterNodeSizes;
		this.clusterEdgeSizes = clusterEdgeSizes;

		for (int i = 0; i < clusterNodeSizes.length; ++i) {
			clusters.add(new ArrayList<Integer>(clusterNodeSizes[i]));
		}
		System.out.println("Putting nodes into new clusters!");
		for (int i = 1; i < clusterMap.length; ++i) {
			if (i % 1000000 == 0) {
				System.out.println("Put nodeId: " + i + " to its cluster...");
			}
			clusters.get(clusterMap[i]).add(i);
		}
	}

	public String toDebugString() {
		String retVal = "";
		for (int i = 0; i < clusterNodeSizes.length; ++i) {
			retVal +=
				"cluster " + i + " nodeSize: " + clusterNodeSizes[i] + " edgeSize: "
					+ clusterEdgeSizes[i] + " edgeDensity: "
					+ (clusterEdgeSizes[i] / clusterNodeSizes[i] + "\n");
		}
		return retVal;
	}

}
