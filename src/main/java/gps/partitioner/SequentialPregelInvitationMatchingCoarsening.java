package gps.partitioner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

// Sequential Pregel Implementation of the Coarsening + METIS partitioning algorithm
// The algorithm first by random matchings coarsens the graph multiple iterations and then
// runs METIS on the coarsest graph.
public class SequentialPregelInvitationMatchingCoarsening {
//
//	public SequentialPregelInvitationMatchingCoarsening(Map<Integer, Map<Integer, Integer>> weightedGraph) {
//		super(weightedGraph);
//	}
//
//	@Override
//	public State getState(int vertexId) {
//		return new InvitationMatchingState(vertexId);
//	}
//
//	public void start() {
//		Random random = new Random();
//		for (int iterationNo = 0; iterationNo < 50; ++iterationNo) {
//			int numWastedAcceptances = 0;
//			int numSuccessfullAcceptances = 0;
//			int numWastedInvitations = 0;
//			int numSuccessfullInvitations = 0;
//
//			for (int superstepNo = 0; superstepNo < 8; superstepNo++) {
////				System.out.println("Starting superstepNo : " + superstepNo);
//				for (Entry<Integer, Map<Integer, Integer>> entry : weightedGraph.entrySet()) {
//					int vertexId = entry.getKey();
//					Map<Integer, Integer> weightedNeighbors = entry.getValue();
//					InvitationMatchingState state = (InvitationMatchingState) statesMap.get(vertexId);
//					List<InvitationMatchingMessage> messageList = (List<InvitationMatchingMessage>) messageListForCurrentSuperstep.get(vertexId);
//					if (superstepNo == 0) {
//						state.isInviting = false;
//						if (state.superNodeId != vertexId) {
//							((List<InvitationMatchingMessage>)messageListForNextSuperstep.get(state.superNodeId)).add(
//								new InvitationMatchingMessage(vertexId));
//						}
//					} else if (superstepNo == 1) {
//						// If a vertex is a superNode itself and the coin flip is < 0.5
//						// then it will send an invitation first to its subVertices.
//						// In the next superstep it and all its subvertices will pass
//						// the invitation to all their neighbors
//						if ((state.superNodeId == vertexId) && (Math.random() < 0.7)) {
//							state.isInviting = true;
//							for (InvitationMatchingMessage message : messageList) {
//								int subVertexId = message.subVertexId;
//								((List<InvitationMatchingMessage>)messageListForNextSuperstep.get(subVertexId)).add(
//									new InvitationMatchingMessage(true));
//							}
//						} else {
//							state.isInviting = false;
//						}
//					} else if (superstepNo == 2) {
//						// In this superstep every subVertex found out whether or not its
//						// superNode is inviting. If so, they will pass on the invitation to
//						// all their neighbors, who in the next superstep will pass it to
//						// their superNodes. Also in this superstep, every superNode that is
//						// inviting passes its invitation to all its neighbors.
//						if (state.isInviting || !messageList.isEmpty()) {
//							checkMessageListHasOnlyOneBooleanMessage(messageList);
//							state.isInviting = true;
//							for (int neighborId : weightedNeighbors.keySet()) {
//								((List<InvitationMatchingMessage>)messageListForNextSuperstep.get(neighborId)).add(
//									new InvitationMatchingMessage(state.superNodeId, false /* dummy boolean */));
//							}
//						} else {
//							// Do nothing, if superNode the vertex is waiting for invitations.
//						}
//					} else if (superstepNo == 3) {
//						if (!state.isInviting && !messageList.isEmpty()) {
//							HashSet<Integer> invitingVertices = new HashSet<Integer>();
//							for (InvitationMatchingMessage message : messageList) {
//								if (message.invitorVertexId < 0) {
//									System.out.println("Inviting message id is not initialized." +
//										"Wrong message");
//								}
//								invitingVertices.add(message.invitorVertexId);
//							}
//							((List<InvitationMatchingMessage>)messageListForNextSuperstep.get(state.superNodeId)).add(
//								new InvitationMatchingMessage(invitingVertices));
//						}
//					} else if (superstepNo == 4) {
//						checkSuperstepNo4MessagesAreConsistent(messageList, state, vertexId);
//						if (!state.isInviting && !messageList.isEmpty()) {
//							HashSet<Integer> allInvitingVertices = new HashSet<Integer>();
//							for (InvitationMatchingMessage message : messageList) {
//								allInvitingVertices.addAll(message.invitingVertices);
//							}
//							List<Integer> listOfAllInvitingVertices = new ArrayList<Integer>(
//								allInvitingVertices);
//							numWastedInvitations += listOfAllInvitingVertices.size() - 1;
//							numSuccessfullInvitations++;
//							Integer acceptedInvitor = listOfAllInvitingVertices.get(
//								random.nextInt(listOfAllInvitingVertices.size()));
////							System.out.println("vertexId: " + vertexId
////								+ " is accepting the invitation from acceptedInvitor: " + acceptedInvitor);
//							((List<InvitationMatchingMessage>)messageListForNextSuperstep.get(acceptedInvitor)).add(
//								new InvitationMatchingMessage(vertexId, 0.1 /* dummy double */));
//						}
//					} else if (superstepNo == 5) {
//						checkSuperstepNo5MessagesAreConsistent(messageList, state, vertexId);
//						if (state.isInviting && !messageList.isEmpty()) {
//							List<Integer> allAcceptedInvitations = new ArrayList<Integer>();
//							numWastedAcceptances += messageList.size() - 1;
//							numSuccessfullAcceptances++;
//							for (InvitationMatchingMessage message : messageList) {
//								allAcceptedInvitations.add(message.acceptingInviteeId);
//							}
//							checkNoDuplicateIds(allAcceptedInvitations, vertexId);
//							Integer acceptedInvitee = allAcceptedInvitations.get(
//								random.nextInt(allAcceptedInvitations.size()));
////							System.out.println("vertexId: " + vertexId
////								+ " is accepting the acceptance from acceptedInvitee: "
////								+ acceptedInvitee);
//							((List<InvitationMatchingMessage>)messageListForNextSuperstep.get(acceptedInvitee)).add(
//								new InvitationMatchingMessage(vertexId, "" /* dummy string */));
//						}
//						if (!state.isInviting && (state.superNodeId != vertexId)) {
//							((List<InvitationMatchingMessage>)messageListForNextSuperstep.get(state.superNodeId)).add(
//								new InvitationMatchingMessage(vertexId, InvitationMatchingMessage.Type.SUPERSTEP_5_SUBVERTEX));
//						}
//					} else if (superstepNo == 6) {
//						checkSuperstepNo6MessagesAreConsistent(messageList, state, vertexId);
//						if (!state.isInviting && !messageList.isEmpty()) {
//							int acceptingInvitorId = -1;
//							List<Integer> subVertices = new ArrayList<Integer>();
//							for (InvitationMatchingMessage message : messageList) {
//								if (InvitationMatchingMessage.Type.SUPERSTEP_5_ACCEPTANCE == message.type) {
//									assert acceptingInvitorId == -1 : " vertexId: "
//										+ vertexId + " already has an accepting invitor";
//									acceptingInvitorId = message.acceptingInvitorId;
//								} else if (InvitationMatchingMessage.Type.SUPERSTEP_5_SUBVERTEX == message.type) {
//									subVertices.add(message.subVertexId);
//								}
//							}
//							if (acceptingInvitorId != -1) {
//								state.superNodeId = acceptingInvitorId;
//								for (int subVertexId : subVertices) {
//									((List<InvitationMatchingMessage>)messageListForNextSuperstep.get(subVertexId))
//										.add(new InvitationMatchingMessage(acceptingInvitorId, Type.SUPERSTEP_6,
//											"" /* dummy string */));
//								}
//							}
//						}
//					} else if (superstepNo == 7) {
//						checkSuperstepNo7messagesAreConsistent(messageList, state, vertexId);
//						if (!messageList.isEmpty()) {
//							state.superNodeId = messageList.get(0).newSuperNodeId;
//						}
//					}
//				}
//				swapCurrentAndNextMessageLists();
//			}
//			System.out.println("IterationNo: " + iterationNo + " numSuccessfullAcceptances: "
//				+ numSuccessfullAcceptances + " numWastedAcceptances: " + numWastedAcceptances);
//			System.out.println("IterationNo: " + iterationNo + " numSuccessfullInvitations: "
//				+ numSuccessfullInvitations + " numWastedInvitations: " + numWastedInvitations);
//			dumpStatesAndCurrentGraphStats();
//		}
//		dumpNumVerticesAndEdgesStats();
//	}
//
//	private void checkNoDuplicateIds(List<Integer> allAcceptedInvitations, int vertexId) {
//		HashSet<Integer> hashSet = new HashSet<Integer>(allAcceptedInvitations);
//		assert (hashSet.size() == allAcceptedInvitations.size()) :
//			"There are duplicate vertices that accepted the invitation from vertexId: " + vertexId +
//			" hashSet.size(): " + hashSet.size() + " allAcceptedInvitations.size(): "
//			+ allAcceptedInvitations.size();
//	}
//
//	private void checkSuperstepNo7messagesAreConsistent(List<InvitationMatchingMessage> messageList, InvitationMatchingState state,
//		int vertexId) {
//		if (state.isInviting) {
//			assert messageList.isEmpty() :
//				"state is inviting but has messages in superstepNo 7. vertexId: " + vertexId;
//		}
//		if (!state.isInviting) {
//			assert messageList.isEmpty() || (messageList.size() == 1) :
//				"state is not inviting and has more than 1 message in superstepNo 7. vertexId: " + vertexId;
//		}
//		
//	}
//
//	private void checkSuperstepNo6MessagesAreConsistent(List<InvitationMatchingMessage> messageList, InvitationMatchingState state,
//		int vertexId) {
//		if (state.isInviting) {
//			assert messageList.isEmpty() :
//				"state is inviting but has messages in superstepNo 6. vertexId: " + vertexId;
//		}
//		if (!state.isInviting && (state.superNodeId != vertexId)) {
//			assert messageList.isEmpty() :
//				"state is not inviting and vertex is not a superNode but it has messages in " +
//				"superstepNo 6. vertexId: " + vertexId;
//		}
////		if (!state.isInviting && (state.superNodeId == vertexId) && !messageList.isEmpty()) {
////			assert messageList.size() == 1 :
////				"state is not inviting and vertex is a superNode but it has more than 1 accepted invitors " +
////				"superstepNo 6. vertexId: " + vertexId;
////		}
//	}
//
//	private void checkSuperstepNo5MessagesAreConsistent(List<InvitationMatchingMessage> messageList, InvitationMatchingState state,
//		int vertexId) {
//		if (!state.isInviting) {
//			assert messageList.isEmpty() :
//				"state is not inviting but has messages in superstepNo 5. vertexId: " + vertexId;
//		}
////		if (state.isInviting && (state.superNodeId != vertexId)) {
////			if (!messageList.isEmpty()) {
////				for (Message message : messageList) {
////					System.out.println(message);
////				}
////			}
////			assert messageList.isEmpty() :
////				"state is inviting and vertex is not a superNode but it has messages in " +
////				"superstepNo 5. vertexId: " + vertexId;
////		}
////		if (state.isInviting && !messageList.isEmpty()) {
////			for (Message message : messageList) {
////				assert message.acceptingInviteeId >= 0 :
////					" message's acceptingVertexId should be a nonnegative value";
////			}
////		}
//	}
//
//	private void checkSuperstepNo4MessagesAreConsistent(
//		List<InvitationMatchingMessage> messageList, InvitationMatchingState state, int vertexId) {
//		if (state.isInviting) {
//			assert messageList.isEmpty() :
//				"state is inviting but has messages in superstepNo 4. vertexId: " + vertexId;
//		}
//		if (!state.isInviting && (state.superNodeId != vertexId)) {
//			assert messageList.isEmpty() :
//				"state is not inviting and vertex is not a superNode but it has messages in " +
//				"superstepNo 4. vertexId: " + vertexId;
//		}
//
//	}
//
//	private void checkMessageListHasOnlyOneBooleanMessage(List<InvitationMatchingMessage> messageList) {
//		if (!messageList.isEmpty()) {
//			if (messageList.size() != 1 ||
//				!messageList.get(0).superstepNoIsInviting) {
//				System.out.println("ERROR. If subVertex has a message, it" +
//					"should only have 1 message and it should be from its " +
//					"superNode with a message.superstepNoIsInviting=true");
//				System.out.println("Found messageListSize: "
//					+ messageList.size());
//				System.out.println("message.superstepNoIsInviting: "
//					+ messageList.get(0).superstepNoIsInviting);
//			}
//		}
//	}
//
//	public static class InvitationMatchingState extends State {
//		public boolean isInviting;
//		
//		public InvitationMatchingState(int vertexId) {
//			super(vertexId);
//		}
//	}
//
//	public static class InvitationMatchingMessage extends Message {
//		
//		public enum Type {
//			SUPERSTEP_0,
//			SUPERSTEP_1,
//			SUPERSTEP_2,
//			SUPERSTEP_3,
//			SUPERSTEP_4,
//			SUPERSTEP_5_ACCEPTANCE,
//			SUPERSTEP_5_SUBVERTEX,
//			SUPERSTEP_6,
//			SUPERSTEP_7,
//		}
//		
//		private final Type type;
//		private int subVertexId = -1;
//		private boolean superstepNoIsInviting = false;
//		private int invitorVertexId = -1;
//		private HashSet<Integer> invitingVertices = null;
//		private int acceptingInviteeId = -1;
//		private int acceptingInvitorId = -1;
//		private int newSuperNodeId;
//
//		public InvitationMatchingMessage(int subVertexId) {
//			this.subVertexId = subVertexId;
//			this.type = Type.SUPERSTEP_0;
//		}
//		
//		public InvitationMatchingMessage(boolean superstepNoIsInviting) {
//			this.superstepNoIsInviting = superstepNoIsInviting;
//			this.type = Type.SUPERSTEP_1;
//		}
//		
//		public InvitationMatchingMessage(int invitorVertexId, boolean foo) {
//			this.invitorVertexId = invitorVertexId;
//			this.type = Type.SUPERSTEP_2;
//		}
//
//		public InvitationMatchingMessage(HashSet<Integer> invitingVertices) {
//			this.invitingVertices = invitingVertices;
//			this.type = Type.SUPERSTEP_3;
//		}
//
//		public InvitationMatchingMessage(int acceptingInviteeId, double foo) {
//			this.acceptingInviteeId = acceptingInviteeId;
//			this.type = Type.SUPERSTEP_4;
//		}
//
//		public InvitationMatchingMessage(int acceptingInvitor, String string) {
//			this.acceptingInvitorId = acceptingInvitor;
//			this.type = Type.SUPERSTEP_5_ACCEPTANCE;
//		}
//		
//		public InvitationMatchingMessage(int subVertexId, Type type) {
//			this.subVertexId = subVertexId;
//			this.type = type;
//		}
//		
//		public InvitationMatchingMessage(int newSuperNodeId, Type type, String foo) {
//			this.newSuperNodeId = newSuperNodeId;
//			this.type = type;
//		}
//
//		@Override
//		public String toString() {
//			System.out.println("Start of outputtting Message...");
//			System.out.print("subVertexId: " + this.subVertexId);
//			System.out.print("\tsuperstepNoIsInviting: " + this.superstepNoIsInviting);
//			System.out.print("\tinvitorVertexId: " + this.invitorVertexId);
//			System.out.print("\tinvitingVertices: " + (this.invitingVertices == null ? "null" : "printing..."));
//			if (this.invitingVertices != null) {
//				for (int invitingVertex : invitingVertices) {
//					System.out.print(" " + invitingVertex);
//				}
//			}
//			System.out.print("\tacceptingInviteeId: " + this.acceptingInviteeId);
//			System.out.print("\tacceptingInvitorId: " + this.acceptingInvitorId);
//			System.out.println("End of outputtting Message...");
//			return "";
//		}
//	}
//	
//	public static void main(String[] args) throws FileNotFoundException, IOException {
//		new SequentialPregelInvitationMatchingCoarsening(
//			PartitionerUtils.convertToWeightedEdges(
//			PartitionerUtils.readSnapFileIntoGraphAndMakeUndirected(args[0]))).start();
//	}
}
