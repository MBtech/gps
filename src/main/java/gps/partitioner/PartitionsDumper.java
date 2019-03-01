package gps.partitioner;

import gps.node.Pair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class PartitionsDumper {

	public static PartitionArraysWrapper dumpPartitionStatistics(String directedinputFile, String partsFile,
		int numPartitions, boolean isMetisPartsFile) throws IOException {
		List<Integer> partitionIds = isMetisPartsFile ? PartitionerUtils.readMETISPartitions(partsFile)
			: readRelabelsMapPartsFile(partsFile, numPartitions);
		int[] numVerticesInPartitions = new int[numPartitions];
		int[] numEdgesInPartitions = new int[numPartitions];
		int[] numIncomingEdgesToPartitions = new int[numPartitions];
		int numEdgesCrossingPartitions = 0;
		int totalNumEdges = 0;
		int vertexPartitionId;
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(directedinputFile);
		String line;
		String[] split;
		int counter = 0;
		long previousTime = System.currentTimeMillis();
		int neighborPartitionId;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed directed input file " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			split = line.split("\\s+");
			vertexPartitionId = partitionIds.get(Integer.parseInt(split[0]));
			numEdgesInPartitions[vertexPartitionId] += (split.length - 1);
			totalNumEdges += (split.length - 1);
			numVerticesInPartitions[vertexPartitionId] += 1;
			for (int i = 1; i < split.length; ++i) {
				neighborPartitionId = partitionIds.get(Integer.parseInt(split[i]));
				if (vertexPartitionId != neighborPartitionId) {
					numEdgesCrossingPartitions++;
				}
				numIncomingEdgesToPartitions[neighborPartitionId]++;
			}
		}

		dumpPartitionStatistics(numPartitions, numVerticesInPartitions, numEdgesInPartitions,
			numIncomingEdgesToPartitions);
		System.out.println("totalNumEdges: " + totalNumEdges);
		System.out.println("numEdgesCrossingPartitions: " + numEdgesCrossingPartitions);
		PartitionArraysWrapper partitionArraysWrapper = new PartitionArraysWrapper();
		partitionArraysWrapper.numVerticesInPartitions = numVerticesInPartitions;
		partitionArraysWrapper.numEdgesInPartitions = numEdgesInPartitions;
		partitionArraysWrapper.numIncomingEdgesToPartitions = numIncomingEdgesToPartitions;
		partitionArraysWrapper.partitionIds = partitionIds;
		return partitionArraysWrapper;
	}

	private static List<Integer> readRelabelsMapPartsFile(String partsFile, int numPartitions)
		throws NumberFormatException, IOException {
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(partsFile);
		String line;
		List<Integer> partitions = new ArrayList<Integer>();
		String[] split;
		while ((line = bufferedReader.readLine()) != null) {
			split = line.split("\\s+");
			int partitionId = Integer.parseInt(split[2]) % numPartitions;
//			System.out.println("vertexId: " + counter + " has partitionId: " + partitionId);
			partitions.add(partitionId);
		}
		bufferedReader.close();
		System.out.println("Read relabelsmap partitionsFile: " + partsFile);
		return partitions;
	}

	private static void dumpPartitionStatistics(int numPartitions, int[] numVerticesInPartitions,
		int[] numEdgesInPartitions, int[] numIncomingEdgesToPartitions) {
		for (int i = 0; i < numPartitions; ++i) {
			System.out.println("partitionNo: " + i + " numVertices: "
				+ numVerticesInPartitions[i] + " numEdges: " + numEdgesInPartitions[i] +
				" numIncomingEdges: " + numIncomingEdgesToPartitions[i]);
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		PartitionArraysWrapper partitionArraysWrapper = dumpPartitionStatistics(args[0], args[1],
			Integer.parseInt(args[2]), Boolean.parseBoolean(args[3]));
		if (Boolean.parseBoolean(args[4])) {
			smoothPartitionsByPuttingEachOneFixedNumberOfSubpartitions(args[6], partitionArraysWrapper,
				Integer.parseInt(args[5]));
		}
	}
	
	private static void smoothPartitionsByPuttingEachOneFixedNumberOfSubpartitions(String outputFile,
		PartitionArraysWrapper partitionArraysWrapper, int finalNumPartitions) throws IOException {
		int numOriginalPartitions = partitionArraysWrapper.numEdgesInPartitions.length;
		List<Pair<Integer, Integer>> partitionsAccordingToNumEdges =
			new ArrayList<Pair<Integer,Integer>>(numOriginalPartitions);
		for (int i = 0; i < numOriginalPartitions; ++i) {
			partitionsAccordingToNumEdges.add(
				new Pair(i, partitionArraysWrapper.numEdgesInPartitions[i]));
		}
		Collections.sort(partitionsAccordingToNumEdges, new ComparatorBySecondInteger());
		System.out.println("Dumping partitions in decreasing order fo numEdgesInPartitions");
		for (int i = 0; i < numOriginalPartitions; ++i) {
			System.out.println(" partitionsId: " + partitionsAccordingToNumEdges.get(i).fst + " numEdges: " +
				partitionArraysWrapper.numEdgesInPartitions[partitionsAccordingToNumEdges.get(i).fst]);
		}
		System.out.println("Done dumping partitions in decreasing order fo numEdgesInPartitions");
		
		int[] newNumVertices = new int[finalNumPartitions];
		int[] newNumEdgesInPartitions = new int[finalNumPartitions];
		int[] newNumIncomingEdgesToPartitions = new int[finalNumPartitions];
		int numPartitionsToSmooth = 4;
		int counter = 0;
		int numSubPartitionsPerPartitions = numOriginalPartitions / finalNumPartitions;
		int[] finalPartitionIds = new int[numOriginalPartitions];
		for (int i = 0; i < finalPartitionIds.length; ++i) {
			finalPartitionIds[i] = -1;
		}
		int[] subPartitionsPerPartition = new int[finalNumPartitions];
		int nextPartitionIdToGive = numPartitionsToSmooth;
		for (int i = 0; i < numOriginalPartitions; ++i) {
//			System.out.println("nextPartitionIdToGive: " + nextPartitionIdToGive + " i: " + i);
			int partitionId = partitionsAccordingToNumEdges.get(i).fst;
			if (i < numPartitionsToSmooth) {
				int nextHeavyPartitionId = partitionId;
				finalPartitionIds[nextHeavyPartitionId] = i;
				addNumVerticesEdgesAndIncomingEdgesToNewPartitions(partitionArraysWrapper,
					newNumVertices, newNumEdgesInPartitions, newNumIncomingEdgesToPartitions,
					partitionId, i);
				subPartitionsPerPartition[i]++;
				for (int j = 1; j <= numSubPartitionsPerPartitions - 1; ++j) {
					int nextLightPartitionId = partitionsAccordingToNumEdges.get(numOriginalPartitions -
						(i*(numSubPartitionsPerPartitions - 1) + j)).fst;
					finalPartitionIds[nextLightPartitionId] = i;
					addNumVerticesEdgesAndIncomingEdgesToNewPartitions(partitionArraysWrapper,
						newNumVertices, newNumEdgesInPartitions, newNumIncomingEdgesToPartitions,
						nextLightPartitionId, i);
					subPartitionsPerPartition[i]++;
				}
			} else if (finalPartitionIds[partitionId] != -1) {
				continue;
			} else {
				if (subPartitionsPerPartition[nextPartitionIdToGive] > numSubPartitionsPerPartitions) {
					System.out.println("This should never happen!!!! subPartitionsPerPartition[nextPartitionIdToGive] > numSubPartitionsPerPartitions."
						+ " subPartitionsPerPartition[nextPartitionIdToGive]: "
						+ subPartitionsPerPartition[nextPartitionIdToGive] + " numSubPartitionsPerPartitions: " + numSubPartitionsPerPartitions);
				}
				finalPartitionIds[partitionId] = nextPartitionIdToGive;
				addNumVerticesEdgesAndIncomingEdgesToNewPartitions(partitionArraysWrapper,
					newNumVertices, newNumEdgesInPartitions, newNumIncomingEdgesToPartitions,
					partitionId, nextPartitionIdToGive);
				subPartitionsPerPartition[nextPartitionIdToGive]++;
				nextPartitionIdToGive = (nextPartitionIdToGive == (finalNumPartitions - 1)) ?
					numPartitionsToSmooth : nextPartitionIdToGive + 1;
			}
		}
		for (int i = 0; i < subPartitionsPerPartition.length; ++i) {
			if (subPartitionsPerPartition[i] != numSubPartitionsPerPartitions) {
				System.out.println("There is a partition: " + i + " that has not been assigned " +
						"exactly numSubPartitionsPerPartitions: " + numSubPartitionsPerPartitions 
						+ " partitions. Actually assigned: " + subPartitionsPerPartition[i]);
			}
		}
		System.out.println("Dumping finalPartitionIds...");
		for (int i = 0; i < numOriginalPartitions; ++i) {
			if (finalPartitionIds[i] == -1) {
				System.out.println("This should never happen!! subPartitionId is not given a finalPartitionId: subPartitionId: "
					+ i + " finalPartitionIds[i]: " + finalPartitionIds[i]);
			}
			System.out.println("" + i + " " + finalPartitionIds[i]);
		}
		dumpPartitionStatistics(finalNumPartitions, newNumVertices, newNumEdgesInPartitions,
			newNumIncomingEdgesToPartitions);
		
		writeNewMetisFile(outputFile, partitionArraysWrapper, finalPartitionIds, finalNumPartitions);
	}

	private static void writeNewMetisFile(String outputFile, PartitionArraysWrapper partitionArraysWrapper,
		int[] finalPartitionIds, int finalNumPartitions) throws IOException {
		int[] finalSubPartitionIds = new int[finalPartitionIds.length];
		int[] nextPartitionIdToGive = new int[finalNumPartitions];
		for (int i = 0; i < nextPartitionIdToGive.length; ++i) {
			nextPartitionIdToGive[i] = i;
		}
		for (int i = 0; i < finalPartitionIds.length; ++i) {
			finalSubPartitionIds[i] = nextPartitionIdToGive[finalPartitionIds[i]];
			nextPartitionIdToGive[finalPartitionIds[i]] += finalNumPartitions;
		}
		for (int i = 0; i < finalSubPartitionIds.length; ++i) {
			System.out.println("originalPartitionId: " + i + " finalSubPartitionId: "
				+ finalSubPartitionIds[i]);
		}
		BufferedWriter bufferedWriter = PartitionerUtils.getBufferedWriter(outputFile);
		for (int i = 0; i < partitionArraysWrapper.partitionIds.size(); ++i) {
			bufferedWriter.write("" + finalSubPartitionIds[partitionArraysWrapper.partitionIds.get(i)]);
			bufferedWriter.write("\n");
		}
		bufferedWriter.close();
	}

	private static void addNumVerticesEdgesAndIncomingEdgesToNewPartitions(PartitionArraysWrapper partitionArraysWrapper, int[] newNumVertices,
		int[] newNumEdgesInPartitions, int[] newNumIncomingEdgesToPartitions, int partitionId, int newPartitionId) {
		newNumVertices[newPartitionId] += partitionArraysWrapper.numVerticesInPartitions[partitionId];
		newNumEdgesInPartitions[newPartitionId] += partitionArraysWrapper.numEdgesInPartitions[partitionId];
		newNumIncomingEdgesToPartitions[newPartitionId] += partitionArraysWrapper.numIncomingEdgesToPartitions[partitionId];
	}
	
	private static class ComparatorBySecondInteger implements Comparator<Pair<Integer, Integer>> {

		@Override
		public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
	        return ((o1.snd > o2.snd) ? -1 : (o1==o2 ? 0 : 1));
		}
	}

	private static class PartitionArraysWrapper {
		int[] numVerticesInPartitions;
		int[] numEdgesInPartitions;
		int[] numIncomingEdgesToPartitions;
		List<Integer> partitionIds;
	}
}
