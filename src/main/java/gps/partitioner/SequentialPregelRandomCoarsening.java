package gps.partitioner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class SequentialPregelRandomCoarsening extends
	SequentialPregel<SequentialPregelRandomCoarsening.RandomCoarseningMessage> {

	private static final String MAX_DEGREE_NEIGHBORS_TO_MERGE_TO_OPT_NAME = "mdntmt";
	private static final String MAX_DEGREE_VERTEX_TO_COARSEN_OPT_NAME = "mdvtc";
	private static final String FIXED_PROBABILITY_OPT_NAME = "fp";
	private static final String OUTPUT_DIRECTORY_OPT_NAME = "od";
	private static final String INPUT_FILE_OPT_NAME = "if";
	private static final String SOME_VERTICES_TO_SOME_NEIGHBORS_COARSENING_TYPE_VALUE = "svtsn";
	private static final String EACH_EDGE_WITH_FIXED_PROBABILITY_COARSENING_TYPE_VALUE = "eewfp";
	private static final String ONE_RANDOM_NEIGHBOR_COARSENING_TYPE_VALUE = "orn";
	private static final String COARSENING_TYPE_OPT_NAME = "ct";
	private static final String NUM_ITERATION_OPT_NAME = "ni";
	private static final String SPARSIFICATION_PROBABILITY_OPT_NAME = "sp";
	private final String inputFileName;
	private Double fixedProbability;
	private Integer maxDegreeOfVertexToCoarsen;
	private Integer maxDegreeNeighborsToMergeTo;
	private int numActiveVerticesForNextSuperstep;
	private int numSkippedVertices = 0;
	private int numSingletonsSkipped = 0;
	private int superstepNo;
	private final String outputDirectory;
	private int randomInt;
	private int counter;
	private final CoarseningType coarseningType;
	private int numIterationsOfCoarsening;
	private Double sparsificationProbability;

	public SequentialPregelRandomCoarsening(String inputFile, String outputDirectory,
		Map<Integer, SPVertex<RandomCoarseningMessage>> weightedGraph, CoarseningType coarseningType) {
		super(weightedGraph);
		this.outputDirectory = outputDirectory;
		this.coarseningType = coarseningType;
		int lastIndexOfPoint = inputFile.lastIndexOf(".");
		this.inputFileName =
			inputFile.substring(inputFile.lastIndexOf("/") + 1,
				lastIndexOfPoint == -1 ? inputFile.length() : lastIndexOfPoint);
		numActiveVerticesForNextSuperstep = 0;
		superstepNo = 0;
	}

	public void setFixedProbability(Double fixedProbability) {
		this.fixedProbability = fixedProbability;
	}

	public void setMaxDegreeOfVertexToCoarsen(Integer maxDegreeOfVertexToCoarsen) {
		this.maxDegreeOfVertexToCoarsen = maxDegreeOfVertexToCoarsen;
	}

	public void setMaxDegreeNeighborsToMergeTo(Integer maxDegreeNeighborsToMergeTo) {
		this.maxDegreeNeighborsToMergeTo = maxDegreeNeighborsToMergeTo;
	}

	public void start() throws IOException {
		Random random = new Random();
		int previousNumActiveVertices = 0;
		List<RandomCoarseningMessage> messageList;
		int vertexId;
		Map<Integer, Integer> weightedNeighbors;
		State state;
		SPVertex spVertex;
		int numMessagesReceived = 0;
		// int maxDegree = findMaxDegreeVertexDegree();
		// System.out.println("MAX_DEGREE in Graph: " + maxDegree);
		// if (maxDegreeOfVertexToCoarsen != Integer.MAX_VALUE) {
		// maxDegreeOfVertexToCoarsen = maxDegree / 10;
		// System.out.println("Set max degree to: " + maxDegreeOfVertexToCoarsen);
		// }
		for (int iterationNo = 0; iterationNo < numIterationsOfCoarsening; ++iterationNo) {
			System.out.println("Starting iterationNo: " + iterationNo);
			superstepNo = 1;
			numSkippedVertices = 0;
			while ((superstepNo == 1)
				|| (numActiveVerticesForNextSuperstep > 0 || previousNumActiveVertices > 0)) {
				numMessagesReceived = 0;
				previousNumActiveVertices = numActiveVerticesForNextSuperstep;
				numActiveVerticesForNextSuperstep = 0;
				numSingletonsSkipped = 0;
				for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry : weightedGraph
					.entrySet()) {
					vertexId = entry.getKey();
					spVertex = entry.getValue();
					weightedNeighbors = spVertex.getWeightedNeighbors();
					state = spVertex.state;
					messageList =
						(List<RandomCoarseningMessage>) messageListForCurrentSuperstep
							.get(vertexId);
					numMessagesReceived += messageList.size();
					if (weightedNeighbors.isEmpty()) {
						// Skipping singletons that have not already merged to another vertexId
						// state.superNodeId = vertexId;
						numSingletonsSkipped++;
						if (spVertex.numEdgesWithinVertex == 0
							|| spVertex.numVerticesWithinVertex == 1) {
							state.skip = true;
						}
						continue;
					}
					if ((superstepNo == 1) && (maxDegreeNeighborsToMergeTo != null)) {
						if (PartitionerUtils.getDegree(weightedNeighbors) <= maxDegreeNeighborsToMergeTo) {
							for (int neighborId : weightedNeighbors.keySet()) {
								((List<RandomCoarseningMessage>) messageListForNextSuperstep
									.get(neighborId)).add(RandomCoarseningMessage
									.newNeighborDegreeNoticeMessage(vertexId,
										PartitionerUtils.getDegree(weightedNeighbors)));
								// System.out.println("vertexId: " + vertexId +
								// " is sending neighborId: "
								// + neighborId + " its degree: " + weightedNeighbors.size());
								// state.superNodeId = Math.min(state.superNodeId , neighborId);
							}
						}
						numActiveVerticesForNextSuperstep++;
					} else if ((superstepNo == 1)
						|| ((maxDegreeNeighborsToMergeTo != null) && superstepNo == 2)) {
						runFirstAssignmentSuperstep(random, messageList, vertexId, superstepNo,
							weightedNeighbors, state);
					} else {
						runTransitiveCoarseningStep(messageList, vertexId, state);
					}
				}
				System.out.println("superstepNo: " + superstepNo
					+ " numActiveVerticesForNextSuperstep: " + numActiveVerticesForNextSuperstep
					+ " numMessagesReceived: " + numMessagesReceived);
				swapCurrentAndNextMessageLists();
				superstepNo++;
			}
			// This will clear the current messages
			swapCurrentAndNextMessageLists();
			// we will run a 3 superstep thing to find the number of vertices on each superNode
			// and the number of edges from each superNode to each other superNode.
			// In the first superstep we clear the next
			runMergingSteps(iterationNo == (numIterationsOfCoarsening - 1));
			// countNumTotalEdges(weightedCoarsenedGraph, coarsenedGraphNumEdgesWithinSupernode);
			// dumpStatesAndCurrentGraphStats();
			System.out.println("numSkippedVertices: " + numSkippedVertices);
			System.out.println("numSingletonsSkipped: " + numSingletonsSkipped);
			System.out.println("totalSkipped: " + (numSkippedVertices + numSingletonsSkipped));
		}
		outputResults();
	}

	private int findMaxDegreeVertexDegree() {
		int maxDegree = -1;
		for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry : weightedGraph.entrySet()) {
			maxDegree =
				Math.max(maxDegree, PartitionerUtils.getDegree(entry.getValue().weightedNeighbors));
		}
		return maxDegree;
	}

	private void outputResults() throws IOException {
		String outputSuffix = "";
		if (fixedProbability != null) {
			outputSuffix += "_fp_" + fixedProbability;
		}
		if (maxDegreeNeighborsToMergeTo != null) {
			outputSuffix += "_mdntmt_" + maxDegreeNeighborsToMergeTo;
		}
		if (maxDegreeOfVertexToCoarsen != null) {
			outputSuffix += "_mdvtc_" + maxDegreeOfVertexToCoarsen;
		}
		if (sparsificationProbability != null) {
			outputSuffix += "_sp_" + sparsificationProbability;
		}
		outputSuffix += "_" + coarseningType.toString().toLowerCase();

		PartitionerUtils.writeWeightedGraphInMETISFormat(outputDirectory + inputFileName
			+ ".vwew_random_partitioning" + outputSuffix + ".mgraph", true, true, true,
			weightedGraph, true /* write original cluster ids and relabels map */);
	}

	private void runMergingSteps(boolean lastIteration) {
		System.out.println("start of running merging steps...");
		// Map<Integer, SPVertex<RandomCoarseningMessage>> weightedCoarsenedGraph =
		// new HashMap<Integer, SPVertex<RandomCoarseningMessage>>();
		List<RandomCoarseningMessage> messageList;
		int vertexId;
		Map<Integer, Integer> weightedNeighbors;
		SPVertex<RandomCoarseningMessage> spVertex;
		State state;
		int numMessagesReceived = 0;
		int numSparsifiedEdges = 0;
		Random random = new Random();
		for (int i = 0; i < 5; ++i) {
			int numSuperNodes = 0;
			int numEdgesCrossingClusters = 0;
			int numEdgesWithinClusters = 0;
			numMessagesReceived = 0;
			for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry : weightedGraph.entrySet()) {
				vertexId = entry.getKey();
				spVertex = entry.getValue();
				weightedNeighbors = spVertex.getWeightedNeighbors();
				state = spVertex.state;
				messageList =
					(List<RandomCoarseningMessage>) messageListForCurrentSuperstep.get(vertexId);
				numMessagesReceived += messageList.size();
				if (((i == 0) || (i == 1)) && weightedNeighbors.isEmpty()) {
					continue;
				}
				if (i == 0) {
					for (Entry<Integer, Integer> weightedNeighbor : weightedNeighbors.entrySet()) {
						((List<RandomCoarseningMessage>) messageListForNextSuperstep
							.get(weightedNeighbor.getKey())).add(RandomCoarseningMessage
							.newNeighborSuperNodeIdNoticeMessage(vertexId, state.superNodeId));
					}
				} else if (i == 1) {
					if (state.superNodeId != vertexId) {
						// System.out.println("vertexId: " + vertexId
						// + " is sending a merged neighbor" + " to you message to: "
						// + state.superNodeId + " numVerticesWithinVertex"
						// + spVertex.numVerticesWithinVertex + " spVertex.numEdgesWithinVertex: "
						// + spVertex.numEdgesWithinVertex);
						((List<RandomCoarseningMessage>) messageListForNextSuperstep
							.get(state.superNodeId)).add(RandomCoarseningMessage
							.newVertexMergedToYouNotice(vertexId, spVertex.numVerticesWithinVertex,
								spVertex.numEdgesWithinVertex));
						spVertex.numEdgesWithinVertex = 0;
						spVertex.numVerticesWithinVertex = 0;
					}
					for (RandomCoarseningMessage message : messageList) {
						if (state.superNodeId == vertexId) {
							// spVertex.numEdgesWithinVertex +=
							// weightedNeighbors.remove(message.vertexId);
						}
						if (sparsificationProbability == null
							|| !lastIteration
							|| (lastIteration && sparsificationProbability != null && random
								.nextDouble() < sparsificationProbability)) {
							((List<RandomCoarseningMessage>) messageListForNextSuperstep
								.get(state.superNodeId)).add(RandomCoarseningMessage
								.newAddNeighborNoticeMessage(message.newSuperNodeId,
									weightedNeighbors.get(message.vertexId)));
						} else {
							numSparsifiedEdges++;
						}
					}
				} else if (i == 2) {
					weightedNeighbors.clear();
					if (state.skip && (vertexId != state.superNodeId)) {
						// System.out.println("SUBNODEID: " + vertexId + " is asking supernode "
						// + state.superNodeId + " if its supernode id has changed.");
						((List<RandomCoarseningMessage>) messageListForNextSuperstep
							.get(state.superNodeId)).add(RandomCoarseningMessage
							.newNotifySubnodeIfSupernodeChanged(vertexId));
						continue;
					}
					if (vertexId == state.superNodeId) {
						// System.out.println("vertexId: " + vertexId + " is a supernode.");
						// weightedCoarsenedGraph.put(vertexId, spVertex);
						numSuperNodes++;
						for (RandomCoarseningMessage message : messageList) {
							switch (message.type) {
							case VERTEX_MERGED_TO_YOU_NOTICE:
								// removing this vertex from neighbors
								// System.out.println(message.vertexId + " HAS MERGED with "
								// + vertexId + " message.numEdgesWithinVertex: " +
								// message.numEdgesWithinVertex);
								// weightedNeighbors.remove(message.vertexId);
								spVertex.numVerticesWithinVertex += message.numVerticesWithinVertex;
								spVertex.numEdgesWithinVertex += message.numEdgesWithinVertex;
								break;
							case ADD_NEIGHBOR_NOTICE:
								if (message.vertexId == vertexId) {
									// spVertex.numEdgesWithinVertex += message.degree;
								} else {
									updateMap(weightedNeighbors, message.vertexId, message.degree);
									numEdgesCrossingClusters += message.degree;
								}
								break;
							}
						}
						// System.out.println("numEdgesWithinClusters before: " +
						// numEdgesWithinClusters
						// + " spVertex.numEdgesWithinVertex: " + spVertex.numEdgesWithinVertex);
						numEdgesWithinClusters += spVertex.numEdgesWithinVertex;
					} else {
						assert messageList.isEmpty() : "if a vertex is assigned to another node,"
							+ "it should not have any messages in the last superstep. step: " + i;
					}
				} else if (i == 3) {
					if (state.superNodeId != vertexId && !messageList.isEmpty()) {
						for (RandomCoarseningMessage message : messageList) {
							// System.out.println("SUPERNODEID: " + vertexId +
							// " is telling subnode "
							// + message.vertexId + " that its new supernode id is: " +
							// state.superNodeId);
							((List<RandomCoarseningMessage>) messageListForNextSuperstep
								.get(message.vertexId)).add(RandomCoarseningMessage
								.newNotifySubnodeIfSupernodeChanged(state.superNodeId));
						}
					}
				} else if (i == 4) {
					if (state.superNodeId != vertexId) {
						state.skip = true;
					}
					if (state.superNodeId != vertexId && !messageList.isEmpty()) {
						assert messageList.size() == 1 : "only the previous supernodeId: "
							+ state.superNodeId + " of vertexId: " + vertexId
							+ " should have notified vertex of subnode change";
						// System.out.println("subvertexId: " + vertexId +
						// " has a new supernodeId: " + messageList.get(0).vertexId);
						state.superNodeId = messageList.get(0).vertexId;
					}
				}
			}
			swapCurrentAndNextMessageLists();
			System.out.println("Finished merging superstepNo: " + i + " numMessagesReceived: "
				+ numMessagesReceived);
			if (i == 1) {
				System.out.println("numSparsifiedEdges: " + numSparsifiedEdges);
			}
			if (i == 2) {
				System.out.println("finished counting superstepNo 2. numSuperNodes: "
					+ numSuperNodes + " numEdgesWithinClusters: " + numEdgesWithinClusters
					+ " numEdgesCrossingClusters: " + numEdgesCrossingClusters + " numTotalEdges: "
					+ (numEdgesWithinClusters + numEdgesCrossingClusters));
			}
		}
		System.out.println("end of running merging steps...");
		// this.weightedGraph = weightedCoarsenedGraph;
	}

	private void runTransitiveCoarseningStep(List<RandomCoarseningMessage> messageList,
		int vertexId, State state) {
		int previousSuperNodeId;
		int newSupernodeId;
		previousSuperNodeId = state.superNodeId;
		newSupernodeId = previousSuperNodeId;
		for (RandomCoarseningMessage message : messageList) {
			newSupernodeId = Math.min(newSupernodeId, message.vertexId);
		}
		state.superNodeId = newSupernodeId;

		if (state.superNodeId != vertexId) {
			for (RandomCoarseningMessage message : messageList) {
				if (!messageListForNextSuperstep.containsKey(message.vertexId)) {
					System.out.println("MESSAGE HAS WRONG VERTEXID: " + message.vertexId);
				}
				((List<RandomCoarseningMessage>) messageListForNextSuperstep.get(message.vertexId))
					.add(RandomCoarseningMessage.newTransitiveSupernodeIdUpdate(newSupernodeId));
			}
		}
		// If value changed, notify the previousSuperNodeId of your newSuperNodeId
		if (previousSuperNodeId != newSupernodeId) {
			((List<RandomCoarseningMessage>) messageListForNextSuperstep.get(previousSuperNodeId))
				.add(RandomCoarseningMessage.newTransitiveSupernodeIdUpdate(newSupernodeId));
			numActiveVerticesForNextSuperstep++;
		}
		// This should be newSuperNodeId
		if (state.superNodeId != vertexId) {
			// If your value did not change, but your superNodeId is not yourself,
			// Notify your superNode with your vertexId, so that if it changes it
			// can notify you
			// TODO(semih): Consider having a new RandomCoarseningMessageType because this message
			// is actually not notifying its supernode, it's asking for a notification from
			// its supernode if its value changes.
			((List<RandomCoarseningMessage>) messageListForNextSuperstep.get(state.superNodeId))
				.add(RandomCoarseningMessage.newTransitiveSupernodeIdUpdate(vertexId));
		} else {
			// Do nothing, you have notified above all vertices that has picked you
			// as a supernode.
		}
	}

	private void runFirstAssignmentSuperstep(Random random,
		List<RandomCoarseningMessage> messageList, int vertexId, int superstepNo,
		Map<Integer, Integer> weightedNeighbors, State state) {
		int mergedNeighborId = -1;
		state.superNodeId = vertexId;
		numActiveVerticesForNextSuperstep++;
		if ((PartitionerUtils.getDegree(weightedNeighbors) > maxDegreeOfVertexToCoarsen)) {
			numSkippedVertices++;
			return;
		}
		switch (coarseningType) {
		case EACH_EDGE_WITH_FIXED_PROBABILITY:
			if (maxDegreeNeighborsToMergeTo != null) {
				for (RandomCoarseningMessage message : messageList) {
					assert message.degree > 0;
					double nextDouble = random.nextDouble();
					// if (message.degree <= maxDegreeNeighborsToMergeTo) {
					if (nextDouble < fixedProbability) {
						System.out.println("HERE");
						((List<RandomCoarseningMessage>) messageListForNextSuperstep
							.get(message.vertexId)).add(RandomCoarseningMessage
							.newVertexMergedToYouNotice(vertexId, -100000, -100000));
						state.superNodeId = Math.min(state.superNodeId, message.vertexId);
					}
					// }
				}
			} else {
				for (int neighborId : weightedNeighbors.keySet()) {
					double nextDouble = random.nextDouble();
					if (nextDouble < fixedProbability) {
						// System.out.println("vertexId: " + vertexId
						// + " is merging with: " + neighborId + " nextDouble: " + nextDouble +
						// " fixedProb: " + fixedProbability);
						((List<RandomCoarseningMessage>) messageListForNextSuperstep
							.get(neighborId)).add(RandomCoarseningMessage
							.newVertexMergedToYouNotice(vertexId, -1000, -1000));
						state.superNodeId = Math.min(state.superNodeId, neighborId);
					}
				}
				break;
			}
		case SOME_VERTICES_TO_SOME_NEIGHBORS:
			if (maxDegreeNeighborsToMergeTo != null) {
				for (RandomCoarseningMessage message : messageList) {
					assert message.degree > 0;
					// if (message.degree <= maxDegreeNeighborsToMergeTo) {

					((List<RandomCoarseningMessage>) messageListForNextSuperstep
						.get(message.vertexId)).add(RandomCoarseningMessage
						.newVertexMergedToYouNotice(vertexId, -10000, -10000));
					state.superNodeId = Math.min(state.superNodeId, message.vertexId);
					// }
				}
			} else {
				for (int neighborId : weightedNeighbors.keySet()) {
					((List<RandomCoarseningMessage>) messageListForNextSuperstep.get(neighborId))
						.add(RandomCoarseningMessage.newVertexMergedToYouNotice(vertexId, -1000000,
							-100000));
					state.superNodeId = Math.min(state.superNodeId, neighborId);
				}
			}
			break;
		case ONE_RANDOM_NEIGHBOR:
			randomInt =
				random.nextInt(maxDegreeNeighborsToMergeTo != null ? messageList.size()
					: weightedNeighbors.size());
			counter = 0;
			if (maxDegreeNeighborsToMergeTo != null) {
				for (RandomCoarseningMessage message : messageList) {
					counter++;
					if (counter == randomInt) {
						((List<RandomCoarseningMessage>) messageListForNextSuperstep
							.get(message.vertexId)).add(RandomCoarseningMessage
							.newVertexMergedToYouNotice(vertexId, -100000, -100000));
						state.superNodeId = Math.min(state.superNodeId, message.vertexId);
					}
				}
			} else {
				for (int neighbor : weightedNeighbors.keySet()) {
					if (counter == randomInt) {
						mergedNeighborId = neighbor;
						break;
					}
					counter++;
				}
				// mergedNeighborId = new ArrayList<Integer>(weightedNeighbors.keySet())
				// .get(randomInt);
				state.superNodeId = Math.min(vertexId, mergedNeighborId);
				// System.out.println("Sending VERTEX_MERGED_TO_YOU_NOTICE message to mergedNeighborId: "
				// + mergedNeighborId);
				((List<RandomCoarseningMessage>) messageListForNextSuperstep.get(mergedNeighborId))
					.add(RandomCoarseningMessage.newVertexMergedToYouNotice(vertexId, -1000000,
						-1000000));
			}
			break;
		}
	}

	// private void countNumTotalEdges(Map<Integer, Map<Integer, Integer>> weightedCoarsenedGraph,
	// Map<Integer, Integer> coarsenedGraphNumEdgesWithinSupernode) {
	// int numEdgesWithinClusters = 0;
	// int numEdgesCrossingClusters = 0;
	// for (Entry<Integer, Map<Integer, Integer>> weightedGraphEntry : weightedCoarsenedGraph
	// .entrySet()) {
	// for (int numEdgesAcrossClusters : weightedGraphEntry.getValue().values()) {
	// numEdgesCrossingClusters += numEdgesAcrossClusters;
	// }
	// }
	//
	// for (int numEdgesWithinSuperNode : coarsenedGraphNumEdgesWithinSupernode.values()) {
	// numEdgesWithinClusters += numEdgesWithinSuperNode;
	// }
	//
	// System.out.println("Start of printing the result of countNumTotalEdges...");
	// System.out.println("numEdgesWithinClusters: " + numEdgesWithinClusters);
	// System.out.println("numEdgesCrossingClusters: " + numEdgesCrossingClusters);
	// System.out.println("numTotalEdges: " + (numEdgesCrossingClusters + numEdgesWithinClusters));
	// System.out.println("End of printing the result of countNumTotalEdges...");
	// }

	private void updateMap(Map<Integer, Integer> map, int key, int degree) {
		if (map.containsKey(key)) {
			map.put(key, map.get(key) + degree);
		} else {
			map.put(key, degree);
		}
	}

	@Override
	public State getState(int vertexId) {
		return new State(vertexId);
	}

	public static class RandomCoarseningMessage extends gps.partitioner.SequentialPregel.Message {
		int vertexId;
		private int degree;
		private int newSuperNodeId;
		private final RandomCoarseningMessageType type;
		private int numVerticesWithinVertex;
		private int numEdgesWithinVertex;

		public RandomCoarseningMessage(RandomCoarseningMessage.RandomCoarseningMessageType type) {
			this.type = type;
		}

		public static RandomCoarseningMessage newNotifySubnodeIfSupernodeChanged(int vertexId) {
			RandomCoarseningMessage retVal =
				new RandomCoarseningMessage(
					RandomCoarseningMessageType.NOTIFY_SUBNODE_IF_SUPERNODE_CHANGED);
			retVal.vertexId = vertexId;
			return retVal;
		}

		public static RandomCoarseningMessage newTransitiveSupernodeIdUpdate(int vertexId) {
			RandomCoarseningMessage retVal =
				new RandomCoarseningMessage(RandomCoarseningMessageType.NEW_MIN_SUPERNODE_ID_NOTICE);
			retVal.vertexId = vertexId;
			return retVal;
		}

		public static RandomCoarseningMessage newAddNeighborNoticeMessage(int vertexId, int degree) {
			RandomCoarseningMessage retVal =
				new RandomCoarseningMessage(RandomCoarseningMessageType.ADD_NEIGHBOR_NOTICE);
			retVal.vertexId = vertexId;
			retVal.degree = degree;
			return retVal;
		}

		public static RandomCoarseningMessage newNeighborDegreeNoticeMessage(int vertexId,
			int degree) {
			RandomCoarseningMessage retVal =
				new RandomCoarseningMessage(RandomCoarseningMessageType.NEIGHBOR_DEGREE_NOTICE);
			retVal.vertexId = vertexId;
			retVal.degree = degree;
			return retVal;
		}

		public static RandomCoarseningMessage newVertexMergedToYouNotice(int vertexId,
			int numVerticesWithinVertex, int numEdgesWithinVertex) {
			RandomCoarseningMessage retVal =
				new RandomCoarseningMessage(RandomCoarseningMessageType.VERTEX_MERGED_TO_YOU_NOTICE);
			retVal.vertexId = vertexId;
			retVal.numVerticesWithinVertex = numVerticesWithinVertex;
			retVal.numEdgesWithinVertex = numEdgesWithinVertex;
			return retVal;
		}

		public static RandomCoarseningMessage newNeighborSuperNodeIdNoticeMessage(int vertexId,
			int newSuperNodeId) {
			RandomCoarseningMessage retVal =
				new RandomCoarseningMessage(
					RandomCoarseningMessageType.NEIGHBOR_SUPERNODE_ID_NOTICE);
			retVal.vertexId = vertexId;
			retVal.newSuperNodeId = newSuperNodeId;
			return retVal;
		}

		enum RandomCoarseningMessageType {
			NEW_MIN_SUPERNODE_ID_NOTICE,
			VERTEX_MERGED_TO_YOU_NOTICE,
			ADD_NEIGHBOR_NOTICE,
			NEIGHBOR_SUPERNODE_ID_NOTICE,
			NEIGHBOR_DEGREE_NOTICE,
			NOTIFY_SUBNODE_IF_SUPERNODE_CHANGED;
		}
	}

	public enum CoarseningType {
		ONE_RANDOM_NEIGHBOR,
		EACH_EDGE_WITH_FIXED_PROBABILITY,
		SOME_VERTICES_TO_SOME_NEIGHBORS,
	}

	public static void main(String[] args) throws FileNotFoundException, IOException,
		ParseException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options
			.addOption(
				COARSENING_TYPE_OPT_NAME,
				"coarseningtype",
				true,
				"coarsening type to use orn (one_random_neighbor) or "
					+ "eewfp (each_edge_with_fixed_probability or svtsn (some_vertices_to_some_neighbors).");
		options.addOption(INPUT_FILE_OPT_NAME, "inputfile", true, "inputFile");
		options.addOption(OUTPUT_DIRECTORY_OPT_NAME, "outputDirectory", true, "outputDirectory");
		options.addOption(FIXED_PROBABILITY_OPT_NAME, "fixedProbability", true,
			"fixed probability value (only if -ct=eewfp)");
		options
			.addOption(
				MAX_DEGREE_VERTEX_TO_COARSEN_OPT_NAME,
				"maxDegreeVertexToCoarsen",
				true,
				"the max degree vertex to coarsen, if a vertex has smaller degree than this value, it"
					+ " will never merge with another vertex, but other lower degree vertices may still "
					+ "choose to merge with it.");
		options
			.addOption(
				MAX_DEGREE_NEIGHBORS_TO_MERGE_TO_OPT_NAME,
				"maxDegreeVertexToCoarsen",
				true,
				"the max degree vertex to coarsen, if a vertex has smaller degree than this value, it"
					+ " will never merge with another vertex, but other lower degree vertices may still "
					+ "choose to merge with it.");
		options.addOption(NUM_ITERATION_OPT_NAME, "numIterations", true,
			"number of iterations of coarsening");
		options.addOption(SPARSIFICATION_PROBABILITY_OPT_NAME, "sparsificationProbability", true,
			"whether to sparsify the graph while merging");
		CommandLine commandLine = parser.parse(options, args);
		CoarseningType coarseningType = getCoraseningType(commandLine);
		String inputFile = commandLine.getOptionValue(INPUT_FILE_OPT_NAME);
		String outputDirectory = commandLine.getOptionValue(OUTPUT_DIRECTORY_OPT_NAME);
		Double fixedProbability = getDoubleValueOrNull(commandLine, FIXED_PROBABILITY_OPT_NAME);
		Integer maxDegreeVertexToCoarsen = getMaxDegreeVertexToCoarsen(commandLine);
		Integer maxDegreeNeighborsToMergeTo = getMaxDegreeNeighborsToMergeTo(commandLine);
		int numIterationsOfCoarsening = getNumIterationsOfCoarsening(commandLine);
		Double sparsificationProbability =
			getDoubleValueOrNull(commandLine, SPARSIFICATION_PROBABILITY_OPT_NAME);

		System.out.println("inputFile: " + inputFile);
		System.out.println("outputDirectory: " + outputDirectory);
		System.out.println("coarseningType: " + coarseningType);
		System.out.println("fixedProbability: " + fixedProbability);
		System.out.println("maxDegreeVertexToCoarsen: " + maxDegreeVertexToCoarsen);
		System.out.println("maxDegreeNeighborsToMergeTo: " + maxDegreeNeighborsToMergeTo);
		System.out.println("numIterationsOfCoarsening: " + numIterationsOfCoarsening);
		System.out.println("sparsificationProbability: " + sparsificationProbability);

		Map<Integer, Map<Integer, Integer>> regularGraph =
			PartitionerUtils.readSnapFileIntoGraphAndMakeUndirectedAndWeighted(inputFile);
		Map<Integer, SPVertex<RandomCoarseningMessage>> weightedGraph =
			PartitionerUtils
				.convertRegularGraphIntoSequentialPregelRandomCoarseningGraph(regularGraph);
		SequentialPregelRandomCoarsening seqPregelRandomCoarsening =
			new SequentialPregelRandomCoarsening(inputFile, outputDirectory, weightedGraph,
				coarseningType);
		seqPregelRandomCoarsening.setFixedProbability(fixedProbability);
		seqPregelRandomCoarsening.setMaxDegreeOfVertexToCoarsen(maxDegreeVertexToCoarsen);
		seqPregelRandomCoarsening.setMaxDegreeNeighborsToMergeTo(maxDegreeNeighborsToMergeTo);
		seqPregelRandomCoarsening.setNumIterationsOfCoarsening(numIterationsOfCoarsening);
		seqPregelRandomCoarsening.setSparsificationProbability(sparsificationProbability);
		seqPregelRandomCoarsening.start();
	}

	private void setSparsificationProbability(Double sparsificationProbability) {
		this.sparsificationProbability = sparsificationProbability;
	}

	private void setNumIterationsOfCoarsening(int numIterationsOfCoarsening) {
		this.numIterationsOfCoarsening = numIterationsOfCoarsening;
	}

	private static int getNumIterationsOfCoarsening(CommandLine commandLine) {
		if (commandLine.hasOption(NUM_ITERATION_OPT_NAME)) {
			return Integer.parseInt(commandLine.getOptionValue(NUM_ITERATION_OPT_NAME));
		}
		return 1;
	}

	private static Integer getMaxDegreeNeighborsToMergeTo(CommandLine commandLine) {
		if (commandLine.hasOption(MAX_DEGREE_NEIGHBORS_TO_MERGE_TO_OPT_NAME)) {
			return Integer.parseInt(commandLine
				.getOptionValue(MAX_DEGREE_NEIGHBORS_TO_MERGE_TO_OPT_NAME));
		}
		return null;
	}

	private static Integer getMaxDegreeVertexToCoarsen(CommandLine commandLine) {
		if (commandLine.hasOption(MAX_DEGREE_VERTEX_TO_COARSEN_OPT_NAME)) {
			return Integer.parseInt(commandLine
				.getOptionValue(MAX_DEGREE_VERTEX_TO_COARSEN_OPT_NAME));
		}
		return Integer.MAX_VALUE;
	}

	private static Double getDoubleValueOrNull(CommandLine commandLine, String optName) {
		if (commandLine.hasOption(optName)) {
			return Double.parseDouble(commandLine.getOptionValue(optName));
		}
		return null;
	}

	private static CoarseningType getCoraseningType(CommandLine commandLine) {
		String coarseningTypeOptValue = commandLine.getOptionValue(COARSENING_TYPE_OPT_NAME);
		if (ONE_RANDOM_NEIGHBOR_COARSENING_TYPE_VALUE.equals(coarseningTypeOptValue)) {
			return CoarseningType.ONE_RANDOM_NEIGHBOR;
		} else if (EACH_EDGE_WITH_FIXED_PROBABILITY_COARSENING_TYPE_VALUE
			.equals(coarseningTypeOptValue)) {
			return CoarseningType.EACH_EDGE_WITH_FIXED_PROBABILITY;
		} else if (SOME_VERTICES_TO_SOME_NEIGHBORS_COARSENING_TYPE_VALUE
			.equals(coarseningTypeOptValue)) {
			return CoarseningType.SOME_VERTICES_TO_SOME_NEIGHBORS;
		} else {
			System.err.println("Unknown value for -cn flag: " + coarseningTypeOptValue);
			System.exit(-1);
		}
		return null;
	}
}
