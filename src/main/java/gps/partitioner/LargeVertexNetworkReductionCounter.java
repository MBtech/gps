package gps.partitioner;

import java.io.BufferedReader;
import java.io.IOException;

public class LargeVertexNetworkReductionCounter {

	public static void main(String[] args) throws IOException {
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(args[0]);
		String line = null;
		String[] split = null;
		int[] numSavedEdges = new int[5];
		int[] numPartitionedVertices = new int[5];
		int numTotalEdges = 0;
		int numTotalVertices = 0;
		int counter = 0;
		int numNeighbors = 0;
		int numActualSavings = 0;
		int numWorkers = 70;
		long previousTime = System.currentTimeMillis();
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			split = line.split("\\s+");
			numNeighbors = split.length - 1;
			numActualSavings = numNeighbors - numWorkers;
			if (numNeighbors > 1000000) {
				numSavedEdges[0] += numActualSavings;
				numPartitionedVertices[0]++;
				numSavedEdges[1] += numActualSavings;
				numPartitionedVertices[1]++;				
				numSavedEdges[2] += numActualSavings;
				numPartitionedVertices[2]++;
				numSavedEdges[3] += numActualSavings;
				numPartitionedVertices[3]++;
				numSavedEdges[4] += numActualSavings;
				numPartitionedVertices[4]++;
			} else if (numNeighbors > 100000) {
				numSavedEdges[0] += numActualSavings;
				numPartitionedVertices[0]++;
				numSavedEdges[1] += numActualSavings;
				numPartitionedVertices[1]++;				
				numSavedEdges[2] += numActualSavings;
				numPartitionedVertices[2]++;
				numSavedEdges[3] += numActualSavings;
				numPartitionedVertices[3]++;
			} else if (numNeighbors > 10000) {
				numSavedEdges[0] += numActualSavings;
				numPartitionedVertices[0]++;
				numSavedEdges[1] += numActualSavings;
				numPartitionedVertices[1]++;				
				numSavedEdges[2] += numActualSavings;
				numPartitionedVertices[2]++;
			} else if (numNeighbors > 1000) {
				numSavedEdges[0] += numActualSavings;
				numPartitionedVertices[0]++;
				numSavedEdges[1] += numActualSavings;
				numPartitionedVertices[1]++;
			} else if (numNeighbors > 100) {
				numSavedEdges[0] += numActualSavings;
				numPartitionedVertices[0]++;
			}
			numTotalEdges += numNeighbors;
			numTotalVertices++;
		}
		System.out.println("numTotalEdges: " + numTotalEdges);
		System.out.println("numTotalVertices: " + numTotalVertices);
		System.out.println("threshold: 100\tnumSavings: " + numSavedEdges[0] + "\tpercentageSavings: " +
			(1.0 - ((double) numSavedEdges[0] / (double) numTotalEdges)) + "\tnumVerticesPartitined: " +
			numPartitionedVertices[0] + "\tpercentagePartitioned: " +
			((double) numPartitionedVertices[0] / (double) numTotalVertices));
		System.out.println("threshold: 1000\tnumSavings: " + numSavedEdges[1] + "\tpercentageSavings: " +
			(1.0 - ((double) numSavedEdges[1] / (double) numTotalEdges)) + "\tnumVerticesPartitined: " +
			numPartitionedVertices[1] + "\tpercentagePartitioned: " +
			((double) numPartitionedVertices[1] / (double) numTotalVertices));
		System.out.println("threshold: 10000\tnumSavings: " + numSavedEdges[2] + "\tpercentageSavings: " +
			(1.0 - ((double) numSavedEdges[2] / (double) numTotalEdges)) + "\tnumVerticesPartitined: " +
			numPartitionedVertices[2] + "\tpercentagePartitioned: " +
			((double) numPartitionedVertices[2] / (double) numTotalVertices));
		System.out.println("threshold: 100000\tnumSavings: " + numSavedEdges[3] + "\tpercentageSavings: " +
			(1.0 - ((double) numSavedEdges[3] / (double) numTotalEdges)) + "\tnumVerticesPartitined: " +
			numPartitionedVertices[3] + "\tpercentagePartitioned: " +
			((double) numPartitionedVertices[3] / (double) numTotalVertices));
		System.out.println("threshold: 1000000\tnumSavings: " + numSavedEdges[4] + "\tpercentageSavings: " +
			(1.0 - ((double) numSavedEdges[4] / (double) numTotalEdges)) + "\tnumVerticesPartitined: " +
			numPartitionedVertices[4] + "\tpercentagePartitioned: " +
			((double) numPartitionedVertices[4] / (double) numTotalVertices));
	}
}
