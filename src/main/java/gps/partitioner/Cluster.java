package gps.partitioner;

public class Cluster {
	public ClusterSummary clusterSummary;
	public Cluster[] clusters;

	public Cluster(ClusterSummary clusterSummary, Cluster[] clusters) {
		this.clusterSummary = clusterSummary;
		this.clusters = clusters;
	}
}
