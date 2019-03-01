package gps.partitioner;

import java.util.HashMap;
import java.util.Map;

public class AffinityPropagationUtils {

	public static String LOGGER_HEADER = "************************";
	public static String getResponsibilityKey(int vertexId, int neighborId) {
		return "r-" + vertexId + "-" + neighborId;
	}

	public static String getAvailabilityKey(int vertexId, int neighborId) {
		return "a-" + vertexId + "-" + neighborId;
	}

	public static void dumpWeightedGraph(Map<Integer, Map<Integer,Integer>> weightedGraph) {
		System.out.println("Start of dumping weighted graph... graphSize: " + weightedGraph.size());
		for (int vertexId: weightedGraph.keySet()) {
//			System.out.println("vertexId: " + vertexId);
			Map<Integer, Integer> weightedNeighbors = weightedGraph.get(vertexId);
			for (int neighborId : weightedNeighbors.keySet()) {
				if (weightedNeighbors.get(neighborId) > 1) {
					System.out.println("w(" + vertexId + ", " + neighborId + "): "
						+ weightedNeighbors.get(neighborId));
				}
			}
		}
		System.out.println("End of dumping weighted graph...");
	}

	public static void dumpResponsibilitiesAndAvailabilities(
		Map<Integer, Map<Integer,Integer>> weightedGraph,
		Map<String, Double> currentAffinitiesMap, Map<String, Double> similaritiesMap, int propationNo) {
		System.out.println(LOGGER_HEADER + " Start of dumping resps and avails propagationNo: "
			+ propationNo + LOGGER_HEADER);
		for (int vertexId : weightedGraph.keySet()) {
			Map<Integer, Integer> weightedNeighbors = weightedGraph.get(vertexId);
			System.out.println("vertex: " + vertexId + " selfAvailability: " + 
				currentAffinitiesMap.get(getAvailabilityKey(vertexId, vertexId))
				+ " selfResponsibility: "
				+ currentAffinitiesMap.get(getResponsibilityKey(vertexId, vertexId))
				+ " selfSimilarity: " + similaritiesMap.get(vertexId + "-" + vertexId));
			for (int neighborId : weightedNeighbors.keySet()) {
				System.out.println("edge: (" + vertexId + ", " + neighborId + "): weight: "
					+ weightedNeighbors.get(neighborId));
				System.out.println("similarity: " +
					similaritiesMap.get(vertexId + "-" + neighborId));
				System.out.println("responsibility: " +
					currentAffinitiesMap.get(getResponsibilityKey(vertexId, neighborId)));
				System.out.println("availability: " +
					currentAffinitiesMap.get(getAvailabilityKey(vertexId, neighborId)));
			}
		}
		System.out.println(LOGGER_HEADER + " End of dumping resps and avails propagationNo: "
			+ propationNo + LOGGER_HEADER);
	}

	public static void dumpCurrentExamplars(Map<Integer, Integer> currentExamplars,
		Map<Integer, Map<Integer, Integer>> graph, int iterationNumber) {
		int numEdgesCrossingClusters = 0;
		int numEdges = 0;
		Map<Integer, Integer> clusters = new HashMap<Integer, Integer>();
		System.out.println(LOGGER_HEADER + " Start of dumping clusters... iterationNo: "
			+ iterationNumber +  " " + LOGGER_HEADER);
		System.out.println("Starting to dump clusters... iterationNo: " + iterationNumber);
		for (int vertexId : graph.keySet()) {
			int examplarId = currentExamplars.get(vertexId);
			if (examplarId == vertexId) {
				// System.out.println("vertexId: " + vertexId + " has picked itself as examplar.");
			} else {
				int examplarsExamplar = currentExamplars.get(examplarId);
				if (examplarsExamplar != examplarId) {
					System.out.println("vertexId: " + vertexId + " has picked: " + examplarId
						+ " but that has picked: " + examplarsExamplar + " as examplar.");
				}
			}
			System.out.println(vertexId + ": " + examplarId);
			if (!clusters.containsKey(examplarId)) {
				clusters.put(examplarId, 1);
			} else {
				clusters.put(examplarId, clusters.get(examplarId) + 1);
			}
			boolean foundExamplarId = examplarId == vertexId;
			for (int neighborId : graph.get(vertexId).keySet()) {
				if (examplarId == neighborId) {
					foundExamplarId = true;
				}
				numEdges++;
				int neighborExamplarId = currentExamplars.get(neighborId);
				if (examplarId != neighborExamplarId) {
					numEdgesCrossingClusters++;
				}
			}
			if (!foundExamplarId) {
				System.out.println("vertexId: " + vertexId + " has examplarId: " + examplarId
					+ " but examplarId is not its neighbor.");
			}
		}
		System.out.println("End of dumping clusters...");
		System.out.println("num clusters: " + clusters.size());
		int sumOfMinInterClusterEdges = 0;
		int sumOfAllClusters = 0;
		for (int clusterId : clusters.keySet()) {
			int clusterSize = clusters.get(clusterId);
			sumOfAllClusters += clusterSize;
			if (clusterSize > 1) {
				// System.out.println("clusterId: " + clusterId + ", size: " + clusterSize);
				sumOfMinInterClusterEdges += (clusterSize - 1) * 2;
			}
		}
		System.out.println("num edges: " + numEdges);
		System.out.println("num edges crossing clusters: " + numEdgesCrossingClusters);
		System.out.println("sum of min cluster edges: " + sumOfMinInterClusterEdges);
		System.out.println("sum of all vertices in clusters: " + sumOfAllClusters);
		System.out.println(LOGGER_HEADER + " End of dumping clusters... iterationNo: "
			+ iterationNumber +  " " + LOGGER_HEADER);
	}
}
//Unused code
//double positiveSumOfResp =
//sumOfPositiveResponsibilitiesAccumulatedToVertexFromNeighbors(vertexId,
//	weightedGraph, currentAffinitiesMap);
//double currentSelfResponsibility =
//currentAffinitiesMap.get(getResponsibilityKey(vertexId, vertexId));
//positiveSumOfResp);
//Pair<Pair<Integer, Double>, Pair<Integer, Double>> twoMaxAvailableAndSimilarNeighbors =
//	getTwoMaxAvailabileAndSimilarNeighbors(vertexId, weightedGraph,
//		currentAffinitiesMap, similaritiesMap);
//if (vertexId == 3) {
//	System.out.println("Printing VertexId 3: currentSelfResponsibility: "
//		+ currentSelfResponsibility);
//}
//responsibility)
//sendResponsibilityOfNeighbor(vertexId, vertexId,
//	twoMaxAvailableAndSimilarNeighbors, nextAffinitiesMap, similaritiesMap);
//setResponsibility(nextAffinitiesMap, vertexId, vertexId, responsibility);
//double availability = (currentSelfResponsibility + positiveSumOfResp);
//double responsibilityOfNeighborToVertex =
//	currentAffinitiesMap.get(getResponsibilityKey(neighborId, vertexId));
//if (responsibilityOfNeighborToVertex > 0) {
//	availability -= responsibilityOfNeighborToVertex;
//}
//setAvailability(nextAffinitiesMap, vertexId, neighborId,
//	Math.min(0, availability));
//						sendResponsibilityOfNeighbor(vertexId, neighborId,
//twoMaxAvailableAndSimilarNeighbors, nextAffinitiesMap, similaritiesMap);
//private static void sendResponsibilityOfNeighbor(int vertexId, int neighborId,
//Pair<Pair<Integer, Double>, Pair<Integer, Double>> twoMaxAvailableAndSimilarNeighbors,
//Map<String, Double> nextAffinitiesMap, Map<String, Double> similaritiesMap) {
//double similarityOfVertexToNeighbor = similaritiesMap.get(vertexId + "-" + neighborId);
//if (twoMaxAvailableAndSimilarNeighbors.fst.fst.intValue() == neighborId
//	&& (twoMaxAvailableAndSimilarNeighbors.snd == null)) {
//	setResponsibility(nextAffinitiesMap, vertexId, neighborId, similarityOfVertexToNeighbor);
//	return;
//}
//double maxAvailableAndSimilarValue =
//	twoMaxAvailableAndSimilarNeighbors.fst.fst.intValue() == neighborId ? twoMaxAvailableAndSimilarNeighbors.snd.snd
//		: twoMaxAvailableAndSimilarNeighbors.fst.snd;
////if (vertexId == 3 && neighborId == 3) {
////	System.out.println("maxAvailableAndSimilarValue: " + maxAvailableAndSimilarValue);
////	System.out.println("fromVertexId: " +
////		(twoMaxAvailableAndSimilarNeighbors.fst.fst.intValue() == neighborId ?twoMaxAvailableAndSimilarNeighbors.snd.fst : twoMaxAvailableAndSimilarNeighbors.fst.fst));
////}
//
//setResponsibility(nextAffinitiesMap, vertexId, neighborId, similarityOfVertexToNeighbor
//	- maxAvailableAndSimilarValue);
//}

//private static Pair<Pair<Integer, Double>, Pair<Integer, Double>> getTwoMaxAvailabileAndSimilarNeighbors(
//	int vertexId, Map<Integer, Map<Integer, Integer>> graph,
//	Map<String, Double> currentAffinitiesMap, Map<String, Double> similaritiesMap) {
//	Pair<Integer, Double> firstPair = null;
//	Pair<Integer, Double> secondPair = null;
//	HashSet<Integer> neighborsPlusCurrentVertex =
//		new HashSet<Integer>(graph.get(vertexId).keySet());
//	neighborsPlusCurrentVertex.add(vertexId);
//	for (int neighborId : neighborsPlusCurrentVertex) {
//		double similarityPlusAvailabilityOfNeighbor =
//			similaritiesMap.get(vertexId + "-" + neighborId)
//				+ currentAffinitiesMap.get(getAvailabilityKey(neighborId, vertexId));
//		Pair<Integer, Double> possibleNewPair =
//			Pair.of(neighborId, similarityPlusAvailabilityOfNeighbor);
//		if (firstPair == null) {
//			firstPair = possibleNewPair;
//		} else if (secondPair == null) {
//			if (firstPair.snd > similarityPlusAvailabilityOfNeighbor) {
//				secondPair = possibleNewPair;
//			} else {
//				secondPair = firstPair;
//				firstPair = possibleNewPair;
//			}
//		} else if (similarityPlusAvailabilityOfNeighbor > firstPair.snd) {
//			secondPair = firstPair;
//			firstPair = possibleNewPair;
//		} else if (similarityPlusAvailabilityOfNeighbor > secondPair.snd) {
//			secondPair = possibleNewPair;
//		}
//	}
//	return Pair.of(firstPair, secondPair);
//}
//double unitNeighborSimilarity = weightedNeighbors.isEmpty() ? -5.0 :
//1.0 / totalWeights;
//double selfSimilarity = weightedNeighbors.isEmpty() ? 5.0 :
//-1.0*unitNeighborSimilarity;// (1.0 - unitNeighborSimilarity) * (1.0 - unitNeighborSimilarity);
//(selfSimilarity - (-1.0 + unitNeighborSimilarity*maxWeightNeighborWeight)));
//-1.0 + neighborWeight * unitNeighborSimilarity);
//(-1.0 + neighborWeight*unitNeighborSimilarity - selfSimilarity));

//private static int findMaxWeightNeighborWeight(Map<Integer, Integer> map) {
//	int maxWeight = 1;
//	for (int weight : map.values()) {
//		if (weight > maxWeight) {
//			maxWeight = weight;
//		}
//	}
//	return maxWeight;
//}