package gps.partitioner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
  
public class AffinityPropagation {

	private static Map<String, Double> similaritiesMap = new HashMap<String, Double>();
	private static Map<String, Double> currentAffinitiesMap;
	private static Map<String, Double> nextAffinitiesMap;
	private static Map<Integer, Map<Integer, Integer>> weightedGraph;
	private static Map<Integer, Integer> currentExamplars;
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		// Note: We're assuming that the graph is undirected for now.
		weightedGraph = PartitionerUtils.convertToWeightedEdges(
			PartitionerUtils.readSnapFileIntoGraphAndMakeUndirected(args[0]));
		System.out.println("numVertices: " + weightedGraph.size());
		currentExamplars = new HashMap<Integer, Integer>();
		int numCoarsingLevels = 1;
		int numPropationIteration = 9;
		for (int j = 0; j < numCoarsingLevels; ++j) {
			currentAffinitiesMap = new HashMap<String, Double>();
			initializeSimilarities();
			initializeCurrentAffinitiesAndResponsibilities();
			for (int i = 0; i < numPropationIteration; ++i) {
				AffinityPropagationUtils.dumpResponsibilitiesAndAvailabilities(weightedGraph,
					currentAffinitiesMap, similaritiesMap, i);
				nextAffinitiesMap = new HashMap<String, Double>();
				for (int vertexId : weightedGraph.keySet()) {
					setNextAvailabilityOfVertexIdToNeighborId(vertexId, vertexId);
					setResponsibility(nextAffinitiesMap, vertexId, vertexId,
						computeResponsibility(false /* not initializing */, vertexId, vertexId));
					for (int neighborId : weightedGraph.get(vertexId).keySet()) {
						setAvailability(nextAffinitiesMap, vertexId, neighborId,
							computeAvailability(vertexId, neighborId));
						setResponsibility(nextAffinitiesMap, vertexId, neighborId,
							computeResponsibility(false /* not initializing */,
								vertexId, neighborId));
					}
				}

				currentAffinitiesMap = nextAffinitiesMap;
				currentExamplars = new HashMap<Integer, Integer>();
				setCurrentExamplars();
//				System.out.println("Dumping everything after coarsening:");
//				dumpResponsibilitiesAndAvailabilities(weightedGraph, currentAffinitiesMap,
//					similaritiesMap);
			}
			AffinityPropagationUtils.dumpCurrentExamplars(currentExamplars, weightedGraph, j);
		}
	}

	private static void initializeSimilarities() {
		similaritiesMap.clear();
		for (int vertexId : weightedGraph.keySet()) {
			Map<Integer, Integer> weightedNeighbors = weightedGraph.get(vertexId);
			setSimilarityOfVertexIdToNeighborId(vertexId, vertexId,
				computeSimilarity(vertexId, vertexId));
			for (int neighborId : weightedNeighbors.keySet()) {
				setSimilarityOfVertexIdToNeighborId(vertexId, neighborId,
					computeSimilarity(vertexId, neighborId));
			}
		}

	}

	private static void setNextAvailabilityOfVertexIdToNeighborId(int vertexId, int vertexId2) {
		setAvailability(nextAffinitiesMap, vertexId, vertexId, 
			computeAvailability(vertexId, vertexId));

	}

	private static void setCurrentExamplars() {
		for (int vertexId : weightedGraph.keySet()) {
			currentExamplars.put(vertexId, findMaxAvailableAndResponsible(vertexId));
		}
	}

	private static Integer findMaxAvailableAndResponsible(int vertexId) {
		System.out.println("Finding max available + responsible for vertexId: " + vertexId);
		int maxId = vertexId;
		double maxValue =
			currentAffinitiesMap.get(
				AffinityPropagationUtils.getAvailabilityKey(vertexId, vertexId))
				+ currentAffinitiesMap.get(
					AffinityPropagationUtils.getResponsibilityKey(vertexId, vertexId));
		System.out.println("initial max value: "+ maxValue);
		for (int neighborId : weightedGraph.get(vertexId).keySet()) {
			double value =
				currentAffinitiesMap.get(
					AffinityPropagationUtils.getAvailabilityKey(neighborId, vertexId))
					+ currentAffinitiesMap.get(
						AffinityPropagationUtils.getResponsibilityKey(vertexId, neighborId));
			System.out.println("value: "+ value);
			if (value > maxValue) {
				maxValue = value;
				maxId = neighborId;
			} else if (value == maxValue) {
//				System.out.println("values are the same for vertexId: " + vertexId + " currentMaxId: " + maxId
//					+ " potentialNextMaxId: " + neighborId);
				if (neighborId < maxId) {
					maxId = neighborId;
					maxValue = value;
				}
			}
		}
		return maxId;
	}

	private static double sumOfPositiveResponsibilitiesAccumulatedToVertexFromNeighbors(
		int vertexId) {
		double sum = 0.0;
//		if (vertexId == 2) {
//			System.out.println("Computing the sum of positive accumulated resps for vertex 2");
//		}
		for (int neighborId : weightedGraph.get(vertexId).keySet()) {
			double resp = currentAffinitiesMap.get(
				AffinityPropagationUtils.getResponsibilityKey(neighborId, vertexId));
//			if (vertexId == 2) {
//				System.out.println("resp from neighborId: " + resp);
//			}
			if (resp > 0.0) {
				sum += resp;
			}
		}
		return sum;
	}

	private static void initializeCurrentAffinitiesAndResponsibilities() {
		mergeExamplarsWithExamplees(weightedGraph, currentExamplars);
		for (int vertexId : weightedGraph.keySet()) {
			Map<Integer, Integer> weightedNeighbors = weightedGraph.get(vertexId);
			setAvailability(currentAffinitiesMap, vertexId, vertexId, 0.0);
			setResponsibility(currentAffinitiesMap, vertexId, vertexId,
				computeResponsibility(true /* is initializing */, vertexId, vertexId));
			for (int neighborId : weightedNeighbors.keySet()) {
				setAvailability(currentAffinitiesMap, vertexId, neighborId, 0.0);
				setResponsibility(currentAffinitiesMap, vertexId, neighborId,
					computeResponsibility(true /* is initializing */, vertexId, neighborId));
			}
		}
	}

	private static void setSimilarityOfVertexIdToNeighborId(int vertexId, int neighborId,
		double similarity) {
		similaritiesMap.put(vertexId + "-" + neighborId, similarity);
	}

	private static Double getSimilarity(int vertexId, int neighborId) {
		return similaritiesMap.get(vertexId + "-" + neighborId);
	}

	private static double computeAvailability(int vertexId, int neighborId) {
		if (currentAffinitiesMap == null) {
			return 0.0;
		}
		double sumOfPositiveResponsibilities =
			sumOfPositiveResponsibilitiesAccumulatedToVertexFromNeighbors(vertexId);
		if (vertexId == neighborId) {
			return sumOfPositiveResponsibilities;
		}
		double responsibilityOfVertexIdForNeighborId = getResponsibility(neighborId, vertexId);
		if (responsibilityOfVertexIdForNeighborId > 0.0) {
			sumOfPositiveResponsibilities -= responsibilityOfVertexIdForNeighborId;
		}
		return Math.min(0.0, getResponsibility(vertexId, vertexId) - sumOfPositiveResponsibilities);
	}

	private static double computeResponsibility(boolean isInitializing, int vertexId,
		int neighborId) {
		double currentResponsibility = -1.0;
		if (!isInitializing) {
			currentResponsibility = getResponsibility(vertexId, neighborId);
		}
		if (vertexId == neighborId) {
			return getSimilarity(vertexId, vertexId) - getMaxSimilarityToNeighbors(vertexId);
		}
		double similarity = getSimilarity(vertexId, neighborId);
		double maxAvailabilityNotIncludingNeighbor =
			getMaxAvailabilityAndSimilarityNotIncludingNeighbor(isInitializing, vertexId,
				neighborId);
		double nextResponsibility = similarity - maxAvailabilityNotIncludingNeighbor;
		if (!isInitializing && (currentResponsibility != nextResponsibility)) {
			System.out.println("Resp(" + vertexId + ", " + neighborId + ") is updated from: "
				+ currentResponsibility + " to: " + nextResponsibility);
		}
		return nextResponsibility;
	}

	private static double getMaxSimilarityToNeighbors(int vertexId) {
		double maxSimilarity = Double.NEGATIVE_INFINITY;
		for (int neighborId : weightedGraph.get(vertexId).keySet()) {
			double neighborSimiliarty = getSimilarity(vertexId, neighborId);
			if (neighborSimiliarty > maxSimilarity) {
				maxSimilarity = neighborSimiliarty;
			}
		}
		return maxSimilarity == Double.NEGATIVE_INFINITY ? 0.0 : maxSimilarity;
	}

	private static double computeSimilarity(int vertexId, int neighborId) {
		Map<Integer, Integer> weightedNeighbors = weightedGraph.get(vertexId);
		if (vertexId == neighborId) {
			return weightedNeighbors.isEmpty() ? -100.0 : -1.0 / weightedNeighbors.size();
		}
		int totalWeights = getTotalWeights(weightedNeighbors);
		int neighborWeight = weightedNeighbors.get(neighborId);
		return totalWeights == 0.0 ? -1.0 : -1.0 + (((double) neighborWeight)/totalWeights);
	}

	private static double getMaxAvailabilityAndSimilarityNotIncludingNeighbor(
		boolean isInitializing, int fromVertexId, int toVertexId) {
		double maxSoFar = Double.NEGATIVE_INFINITY;
		Map<Integer, Integer> weightedNeighbors = weightedGraph.get(fromVertexId);
//		if (fromVertexId != toVertexId) {
//			maxSoFar = getAvailability(fromVertexId, fromVertexId) +
//				getSimilarity(fromVertexId, fromVertexId);
//		}
		for (Entry<Integer, Integer> weightedNeighbor : weightedNeighbors.entrySet()) {
			int neighborId = weightedNeighbor.getKey().intValue();
			if (neighborId == toVertexId) {
				continue;
			}
			// Warning: We give the neighborId first because we're looking at the
			// availability of the neighbor to the vertexId
			double availability = isInitializing ? 0.0 : getAvailability(
				neighborId, fromVertexId);
			double similarity = getSimilarity(fromVertexId, neighborId);
			double sum = availability + similarity;
			if (sum > maxSoFar) {
				maxSoFar = sum;
			}
		}
		return maxSoFar == Double.NEGATIVE_INFINITY ? 0.0 : maxSoFar;
	}

	private static double getResponsibility(int vertexId, int neighborId) {
		return currentAffinitiesMap.get(
			AffinityPropagationUtils.getResponsibilityKey(vertexId, neighborId));
	}

	private static double getAvailability(int vertexId, int neighborId) {
		return currentAffinitiesMap.get(
			AffinityPropagationUtils.getAvailabilityKey(vertexId, neighborId));
	}

	private static int getTotalWeights(Map<Integer, Integer> neighborIdsToWeights) {
		int sum = 0;
		for (int value : neighborIdsToWeights.values()) {
			sum += value;
		}
		return sum;
	}

	private static void mergeExamplarsWithExamplees(Map<Integer, Map<Integer, Integer>> graph,
		Map<Integer, Integer> currentExamplars) {
		System.out.println("merging examplars with examplees... graphSizeBefore: " + graph.size());
		int numEdgesCoveredThroughNeighborsWithSameExamplars = 0;
		for (Entry<Integer, Integer> examplarEntry : currentExamplars.entrySet()) {
			int vertexId = examplarEntry.getKey();
			int examplarId = examplarEntry.getValue();
			Map<Integer, Integer> weightedNeighbors = graph.get(vertexId);
			Map<Integer, Integer> weightedNeighborsOfExamplar = graph.get(examplarId);
			HashSet<Integer> neighborsToRemove = new HashSet<Integer>();
			Map<Integer, Integer> iterationNeighbors = new HashMap<Integer, Integer>();
			iterationNeighbors.putAll(weightedNeighbors);
			for (Entry<Integer, Integer> entry : iterationNeighbors.entrySet()) {
				int neighborId = entry.getKey();
				int weight = entry.getValue();
				int neighborExamplarId = currentExamplars.get(neighborId);
				if (neighborId == neighborExamplarId && vertexId == examplarId) {
					// If the neighbor has assigned itself to itself, and the current
					// vertex has assigned itself to itself, then there is nothing to do.
					continue;
				}
				if (neighborExamplarId != examplarId) {
					if (!weightedNeighborsOfExamplar.containsKey(neighborExamplarId)) {
						weightedNeighborsOfExamplar.put(neighborExamplarId, weight);
					} else {
						weightedNeighborsOfExamplar.put(neighborExamplarId,
							weightedNeighborsOfExamplar.get(neighborExamplarId) + weight);
					}
				} else {
					numEdgesCoveredThroughNeighborsWithSameExamplars += weight;
				}
				if (neighborExamplarId != neighborId) {
					neighborsToRemove.add(neighborId);
				}
			}
			if (vertexId != examplarId) {
				graph.remove(vertexId);
			} else {
				for (int neighborIdToRemove: neighborsToRemove) {
					weightedNeighbors.remove(neighborIdToRemove);
				}
			}
		}
		System.out.println("graphSizeAfter: " + graph.size());
		System.out.println("numEdgesCoveredThroughNeighborsWithSameExamplars: "
			+ numEdgesCoveredThroughNeighborsWithSameExamplars);
	}

	private static void setResponsibility(Map<String, Double> affinitiesMap, int vertexId,
		int neighborId, double responsibility) {
		affinitiesMap.put(AffinityPropagationUtils.getResponsibilityKey(vertexId, neighborId),
			responsibility);
	}

	private static Double setAvailability(Map<String, Double> affinitiesMap, int vertexId,
		int neighborId, double availability) {
		return affinitiesMap.put(AffinityPropagationUtils.getAvailabilityKey(vertexId, neighborId),
			availability);
	}
}