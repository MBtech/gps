package gps.node;

//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.Map;
//import java.util.Queue;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.ConcurrentMap;
//
//import org.easymock.internal.matchers.ArrayEquals;
//
//import static pregel.TestUtils.*;
import gps.BaseGPSTest;
//import pregel.examples.PageRankVertex.PageRankVertexFactory;
//import pregel.node.GPSMessage;
//import pregel.node.GPSNode;
//import pregel.node.communication.MessageSenderAndReceiver;
//import pregel.node.communication.MessageSenderAndReceiverFactory;
//import pregel.zookeeper.ZooKeeperUtils;
//
//
//import static org.easymock.EasyMock.*;
//
///**
// * Running test case is 3 machines: machine0, machine1, machine2, and 5 nodes.
// * Machine 0 => this is the local machine and holds nodeId2, and nodeId3 and nodeId5
// * Machine 1 => another machine in the network. contains nodeId4
// * Machine 2 => another machine in the network, contains nodeId5 and nodeId6
// * Graph Structure (in adjacency list format):
// * nodeId1: (not important as it won't be in this partition)
// * nodeId2: nodedId4
// * nodeId3: (no outgoing edges)
// * nodeId4: (not important as it won't be in this partition)
// * nodeId5: nodeId1, nodeId2, nodeId3, nodeId4, nodeId6, nodeId7
// * nodeId6: (not important as it won't be in this partition)
// * nodeId7: (not important as it wont' be in this partition)
// * 
// * Warning: Note that when a test runs {@code GPSNode#DoSuperstepComputationAndSendMessages(int)}
// * the eventual pagerank values will not be correct for the actual graph. This is because there
// * is a single GPSNode running only the pagerank computation on its own partition. So the 
// * messages that should be coming from other machines are not simulated in these tests.
// * @author semihsalihoglu
// *
// */

public class GPSNodeTests extends BaseGPSTest {
	// private GPSNode GPSNode;
	// private ZooKeeperUtils mockZooKeeperUtils;
	//
	// private int graphSize = 5;
	// private int nodeId1 = 5; // machine 2
	// private int nodeId2 = 834063; // machine 0
	// private int nodeId3 = 18603468; // machine 0
	// private int nodeId4 = 215821; // machine 1
	// private int nodeId5 = 555555; // machine 0
	// private int nodeId6 = 555557; // machine 2
	// private int nodeId7 = 215822; // machine 2
	// private byte machineOfNodeId1 = (byte) (nodeId1 % 3);
	// private byte machineOfNodeId4 = (byte) (nodeId4 % 3);
	// private byte machineOfNodeId6 = (byte) (nodeId6 % 3);
	// private byte machineOfNodeId7 = (byte) (nodeId7 % 3);
	// private Map<Integer, Vertex> graphPartition;
	// private ConcurrentMap<Integer, Queue<GPSMessage>> expectedIncomingMessageQueueMap;
	// private ConcurrentMap<Byte, Queue<GPSMessage>> expectedOutboundMessageQueueMap;
	// // TODO(semih): We can mock this queue if we want. Right now we're just passing in a real
	// // instance of the queue to GPSNode.
	// private ConcurrentHashMap<Byte, Queue<GPSMessage>> outboundMessageQueueMap;
	// private Map<Integer, int[]> expectedMachineCommunicationHistogramMap;
	// private Map<Integer, Byte> expectedNodesToShuffle;
	// private PageRankVertexFactory pageRankVertexFactory = new PageRankVertexFactory();
	// private MessageSenderAndReceiverFactory mockMessageSenderAndReceiverFactory;
	// private MessageSenderAndReceiver mockMessageSenderAndReceiver;
	//
	// @Override
	// protected void setUpRest() {
	// super.setUpRest();
	// mockZooKeeperUtils = mocksControl.createMock(ZooKeeperUtils.class);
	// mockMessageSenderAndReceiverFactory =
	// mocksControl.createMock(MessageSenderAndReceiverFactory.class);
	// mockMessageSenderAndReceiver = mocksControl.createMock(MessageSenderAndReceiver.class);
	// outboundMessageQueueMap = new ConcurrentHashMap<Byte, Queue<GPSMessage>>();
	// for (byte machineId : machineConfig.getAllMachineIds()) {
	// if (machineId != localMachineId) {
	// outboundMessageQueueMap.put(machineId, new LinkedList<GPSMessage>());
	// }
	// }
	//
	// graphPartition = new HashMap<Integer, Vertex>();
	// graphPartition.put(nodeId2, pageRankVertexFactory.newInstance(nodeId2, new
	// ArrayList<Integer>(Arrays.asList(nodeId4))));
	// graphPartition.put(nodeId3, pageRankVertexFactory.newInstance(nodeId3, new
	// ArrayList<Integer>()));
	// graphPartition.put(nodeId5, pageRankVertexFactory.newInstance(nodeId5, new
	// ArrayList<Integer>(
	// Arrays.asList(nodeId3, nodeId1, nodeId2, nodeId4, nodeId6, nodeId7))));
	// expectedIncomingMessageQueueMap = new ConcurrentHashMap<Integer, Queue<GPSMessage>>();
	// expectedIncomingMessageQueueMap.put(nodeId2, new ConcurrentLinkedQueue<GPSMessage>());
	// expectedIncomingMessageQueueMap.put(nodeId3, new ConcurrentLinkedQueue<GPSMessage>());
	// expectedIncomingMessageQueueMap.put(nodeId5, new ConcurrentLinkedQueue<GPSMessage>());
	// expectedOutboundMessageQueueMap = new ConcurrentHashMap<Byte, Queue<GPSMessage>>();
	// expectedOutboundMessageQueueMap.put(otherMachineId1, new LinkedList<GPSMessage>());
	// expectedOutboundMessageQueueMap.put(otherMachineId2, new LinkedList<GPSMessage>());
	//
	// expectedMachineCommunicationHistogramMap = new HashMap<Integer, int[]>();
	// expectedNodesToShuffle = new HashMap<Integer, Byte>();
	// }
	//
	// private void expectInitialCallsToFactoryClasses() {
	// expect(mockMessageSenderAndReceiverFactory.newInstance(
	// machineConfig, localMachineId)).andReturn(mockMessageSenderAndReceiver);
	// //
	// expect(mockMessageSenderAndReceiver.getOutboundMessageQueueMap()).andReturn(outboundMessageQueueMap);
	// }
	//
	// private void constructGPSNode(boolean isDynamic) {
	// GPSNode = new GPSNode(localMachineId, machineConfig, graphPartition, null /* pass in a vertex
	// factory */,
	// graphSize, "dummyZKservers",
	// 5000 /* outgoing data buffer sizes */, isDynamic, mockUtils, mockZooKeeperUtils,
	// "dummy-output-file", mockMessageSenderAndReceiverFactory);
	// }
	// public void testConstructorInitsDataStructuresCorrectly() {
	// expectInitialCallsToFactoryClasses();
	// easyMockSupport.replayAll();
	// constructGPSNode(false);
	// // assertIncomingMessageQueueMapsAreEqual(expectedIncomingMessageQueueMap,
	// // GPSNodeInternalGlobalVariables.incomingMessageQueueMap);
	// // assertOutboundMessageQueueMapsAreEqual(expectedOutboundMessageQueueMap,
	// // GPSNodeInternalGlobalVariables.outboundMessageQueueMap);
	// easyMockSupport.verifyAll();
	// }
	//
	// public void
	// testDoSuperstepComputationAndSendMessagesEmptyIncomingMessageQueueEmptyOutboundMessageQueue()
	// {
	// expectInitialCallsToFactoryClasses();
	// int superstepNo = 1;
	// addExpectedMessagesFromNodeId2(superstepNo, 0.0);
	// addExpectedMessagesFromNodeId5(superstepNo, 0.0);
	// easyMockSupport.replayAll();
	// constructGPSNode(false);
	// runDoSuperstepComputationAndSendMessagesAndAssertMessageQueueMaps(superstepNo);
	// easyMockSupport.verifyAll();
	// }
	//
	// // Note: This case is already tested in any of the other tests but it's good to have it
	// // as a separate test case to be able to identify the case when this is the reason for other
	// // tests to fail easily.
	// public void
	// testDoSuperstepComputationAndSendMessagesPutsMessagesForTheSameMachineDirectlyToTheIncomingMessagesQueue()
	// {
	// expectInitialCallsToFactoryClasses();
	// int superstepNo = 4;
	// // Because nodeId2 and nodeId3 are already on the local machine, these messages should
	// // directly go to their incoming queues.
	// double pageRankUpdateValueForNode5 = getPageRankUpdateValue(nodeId5, 0.0);
	// expectedIncomingMessageQueueMap.get(nodeId2).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId2,
	// pageRankUpdateValueForNode5));
	// expectedIncomingMessageQueueMap.get(nodeId3).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId3,
	// pageRankUpdateValueForNode5));
	// easyMockSupport.replayAll();
	// constructGPSNode(false);
	// GPSNode.doSuperstepComputation(superstepNo);
	// // assertIncomingMessageQueueMapsAreEqual(expectedIncomingMessageQueueMap,
	// // GPSNodeInternalGlobalVariables.incomingMessageQueueMap);
	// easyMockSupport.verifyAll();
	// }
	//
	// public void
	// testDoSuperstepComputationAndSendMessagesErrorsOnNonEmptyOutboundMessageQueueMap() {
	// expectInitialCallsToFactoryClasses();
	// //
	// GPSNodeInternalGlobalVariables.outboundMessageQueueMap.get(otherMachineId1).add(GPSMessage.newDataMessage(1,
	// // localMachineId, 62356342, 0.235));
	// easyMockSupport.replayAll();
	// constructGPSNode(false);
	// try {
	// GPSNode.doSuperstepComputation(2);
	// fail("GPSNode should have thrown an AssertionException because outbound" +
	// "messages is not empty");
	// } catch (AssertionError e) {
	// easyMockSupport.verifyAll();
	// }
	// }
	//
	// public void
	// testDoSuperstepComputationAndSendMessagesNonEmptyIncomingMessageQueueEmptyOutboundMessageQueue()
	// {
	// // TODO(semih): This is a big hack! Try to clean this.
	// expectInitialCallsToFactoryClasses();
	// easyMockSupport.replayAll();
	// constructGPSNode(false);
	//
	// int previousSuperstepNo = 74;
	// int currentSuperstepNo = 75;
	// GPSMessage incomingMessageToNode2 = GPSMessage.newDataMessage(previousSuperstepNo,
	// otherMachineId2, nodeId2, 0.44);
	// GPSMessage incomingMessageToNode5_1 = GPSMessage.newDataMessage(previousSuperstepNo,
	// otherMachineId1, nodeId5, 0.1);
	// GPSMessage incomingMessageToNode5_2 = GPSMessage.newDataMessage(previousSuperstepNo,
	// otherMachineId2, nodeId5, 0.21);
	// GPSMessage incomingMessageToNode5_3 = GPSMessage.newDataMessage(previousSuperstepNo,
	// otherMachineId2, nodeId5, 0.8);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5_1);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId2).add(incomingMessageToNode2);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5_2);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5_3);
	//
	// // All incoming messages from the previous superstep should be cleared and only the local
	// // message updates should be passed.
	// addExpectedMessagesFromNodeId2(currentSuperstepNo, 0.44);
	// addExpectedMessagesFromNodeId5(currentSuperstepNo, 0.1 + 0.21 + 0.8);
	//
	// runDoSuperstepComputationAndSendMessagesAndAssertMessageQueueMaps(currentSuperstepNo);
	// easyMockSupport.verifyAll();
	// }
	//
	// public void
	// testDoSuperstepComputationAndSendMessagesDynamicUpdatesMachineCommunicationHistogramCorrectly()
	// throws IOException {
	// expectInitialCallsToFactoryClasses();
	// int superstepNo = 7;
	// initExpectedMachineCommunicationHistogram();
	// addExpectedMessagesFromNodeId2(superstepNo, 0.0);
	// addExpectedMessagesFromNodeId5(superstepNo, 0.0);
	// updateExpectedMachineCommunicationHistgoramForNode2();
	// updateExpectedMachineCommunicationHistgoramForNode5();
	// easyMockSupport.replayAll();
	// constructGPSNode(true);
	// runDoSuperstepComputationAndSendMessagesAndAssertMessageQueueMaps(superstepNo);
	//
	// // assertMachineCommunicationHistogramMapsAreEqual(expectedMachineCommunicationHistogramMap,
	// // GPSNodeInternalGlobalVariables.machineCommunicationHistogramMap);
	// assertNodesToShuffleAreEqual(expectedNodesToShuffle,
	// GPSNodeInternalGlobalVariables.potentialNodesToShuffle);
	// easyMockSupport.verifyAll();
	// }
	//
	// public void testDoSuperstepComputationAndSendMessagesDynamicUpdatesNodesToShuffleCorrectly()
	// throws IOException {
	// expectInitialCallsToFactoryClasses();
	// easyMockSupport.replayAll();
	// constructGPSNode(true);
	//
	// int previousSuperstepNo = 6;
	// int superstepNo = 7;
	// initExpectedMachineCommunicationHistogram();
	// // node5 by default sends 2 messages to its current machine and 3 messages to machine 2
	// // so if there is a single incoming message from machine 2 and a single message from machine
	// 0
	// // it SHOULD NOT shuffle itself, however if there is only a single message from machine 2
	// // and no incoming messages from machine 0 it SHOULD not shuffle itself to machine 2.
	// GPSMessage incomingMessageToNode5FromMachine2 =
	// GPSMessage.newDataMessage(previousSuperstepNo,
	// (byte) 2, nodeId5, 0.1);
	// GPSMessage incomingMessageToNode5FromMachine1 =
	// GPSMessage.newDataMessage(previousSuperstepNo,
	// (byte) 1, nodeId5, 0.1);
	// GPSMessage incomingMessageToNode5FromMachine0 =
	// GPSMessage.newDataMessage(previousSuperstepNo,
	// (byte) 0, nodeId5, 0.1);
	// // In the following case, it should shuffle itself to machine 2.
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5FromMachine2);
	// addExpectedMessagesFromNodeId2(superstepNo, 0.0);
	// addExpectedMessagesFromNodeId5(superstepNo, 0.0);
	// expectedNodesToShuffle.put(nodeId5, (byte) 2);
	//
	// GPSNode.doSuperstepComputation(superstepNo);
	// assertNodesToShuffleAreEqual(expectedNodesToShuffle,
	// GPSNodeInternalGlobalVariables.potentialNodesToShuffle);
	// // assertTrue(GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).isEmpty());
	//
	// clearGPSNodesOutboundMessageQueues();
	// // In this case it should not shuffle itself.
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5FromMachine2);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5FromMachine0);
	// GPSNode.doSuperstepComputation(superstepNo);
	// assertNodesToShuffleAreEqual(expectedNodesToShuffle,
	// GPSNodeInternalGlobalVariables.potentialNodesToShuffle);
	//
	// clearGPSNodesOutboundMessageQueues();
	// // In the following case, when there are enough messages from machine 1 to
	// // overweigh the other messages it should also shuffle itself:
	// // messages to/from machine 0 = 2 sent + 1 received = 4
	// // messages to/from machine 1 = 1 sent + 4 received = 5
	// // messages to/from machine 2 = 3 sent + 0 received = 3
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5FromMachine0);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5FromMachine1);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5FromMachine1);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5FromMachine1);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId5).add(incomingMessageToNode5FromMachine1);
	// expectedNodesToShuffle.put(nodeId5, (byte) 1);
	// GPSNode.doSuperstepComputation(superstepNo);
	// assertNodesToShuffleAreEqual(expectedNodesToShuffle,
	// GPSNodeInternalGlobalVariables.potentialNodesToShuffle);
	// easyMockSupport.verifyAll();
	// }
	//
	// private void updateExpectedMachineCommunicationHistgoramForNode2() {
	// int[] nodeId2Histogram = expectedMachineCommunicationHistogramMap.get(nodeId2);
	// nodeId2Histogram[machineOfNodeId4]++;
	// }
	//
	// private void updateExpectedMachineCommunicationHistgoramForNode5() {
	// int[] nodeId5Histogram = expectedMachineCommunicationHistogramMap.get(nodeId5);
	// nodeId5Histogram[0] = 2;
	// nodeId5Histogram[machineOfNodeId1]++;
	// nodeId5Histogram[machineOfNodeId4]++;
	// nodeId5Histogram[machineOfNodeId6]++;
	// nodeId5Histogram[machineOfNodeId7]++;
	// }
	//
	// private void initExpectedMachineCommunicationHistogram() {
	// expectedMachineCommunicationHistogramMap.put(nodeId2, new int[3]);
	// expectedMachineCommunicationHistogramMap.put(nodeId3, new int[3]);
	// expectedMachineCommunicationHistogramMap.put(nodeId5, new int[3]);
	// }
	//
	// public void testMultipleCallsToDoSuperstepComputationAndSendMessages() {
	// expectInitialCallsToFactoryClasses();
	// easyMockSupport.replayAll();
	// constructGPSNode(false);
	// GPSNode.doSuperstepComputation(3);
	// clearGPSNodesOutboundMessageQueues();
	// GPSNode.doSuperstepComputation(4);
	// clearGPSNodesOutboundMessageQueues();
	// GPSNode.doSuperstepComputation(5);
	// clearGPSNodesOutboundMessageQueues();
	// // Warning: See the warning above in the class definition.
	// // Note: the reason the sum for node5 is 0.0 is because it doesn't have incoming edges
	// // and therefore never gets any update values, so its sum remains 0.0 at every superstep
	// // the reason the sum for node2 is getPageRankUpdateValue(nodeId5, 0.0) is because the
	// // only edge to node2 is from node5 and its update value will be the return value of
	// // getPageRankUpdateValue(nodeId5, 0.0).
	// addExpectedMessagesFromNodeId2(6, getPageRankUpdateValue(nodeId5, 0.0));
	// addExpectedMessagesFromNodeId5(6, 0.0);
	// runDoSuperstepComputationAndSendMessagesAndAssertMessageQueueMaps(6);
	// easyMockSupport.verifyAll();
	// }
	//
	// public void testGlobalNumMessagesSentHistogramHasCorrectValues() {
	// expectInitialCallsToFactoryClasses();
	// easyMockSupport.replayAll();
	// constructGPSNode(false);
	// GPSNode.doSuperstepComputation(24);
	// clearGPSNodesOutboundMessageQueues();
	// GPSNode.doSuperstepComputation(25);
	// clearGPSNodesOutboundMessageQueues();
	// GPSNode.doSuperstepComputation(26);
	// clearGPSNodesOutboundMessageQueues();
	// GPSNode.doSuperstepComputation(27);
	// clearGPSNodesOutboundMessageQueues();
	// GPSNode.doSuperstepComputation(28);
	// clearGPSNodesOutboundMessageQueues();
	// GPSNode.doSuperstepComputation(29);
	// assertEquals(6, GPSNodeInternalGlobalVariables.globalNumMessagesToSendHistogram.size());
	// for (int numMessagesSent :
	// GPSNodeInternalGlobalVariables.globalNumMessagesToSendHistogram.values()) {
	// assertEquals(5 /* total number of edges out of node2, node3 and node5 to other machines*/,
	// numMessagesSent);
	// }
	// easyMockSupport.verifyAll();
	// }
	//
	// public void testStartNodeEntersTheSuperstepBarrier() {
	// //TODO(semih): Implement
	// }
	//
	// public void testStartNodeLeavesAllSuperstepBarriers() {
	// //TODO(semih): Implement
	// }
	//
	// public void testStartNodeEntersNodeExchangeBarrierWhenDynamismOn() {
	// //TODO(semih): Implement
	// }
	//
	// public void testStartNodeDoesNotEnterNodeExchangeBarrierWhenDynamismOff() {
	// //TODO(semih): Implement
	// }
	//
	// private void clearGPSNodesOutboundMessageQueues() {
	// // for (Queue<GPSMessage> outputboundMessageQueue :
	// // GPSNodeInternalGlobalVariables.outboundMessageQueueMap.values()) {
	// // outputboundMessageQueue.clear();
	// // }
	// }
	//
	// // TODO(semih): We might want to change this behavior to ignoring stale incoming messages
	// public void testDoSuperstepComputationAndSendMessagesErrorsOnStaleIncomingMessages() {
	// expectInitialCallsToFactoryClasses();
	// int staleSuperstepNo = 65;
	// int currentSuperstepNo = 75;
	// GPSMessage incomingMessageToNode2 = GPSMessage.newDataMessage(staleSuperstepNo,
	// otherMachineId2, nodeId2, 0.12);
	// //
	// GPSNodeInternalGlobalVariables.incomingMessageQueueMap.get(nodeId2).add(incomingMessageToNode2);
	// easyMockSupport.replayAll();
	// constructGPSNode(false);
	// try {
	// runDoSuperstepComputationAndSendMessagesAndAssertMessageQueueMaps(currentSuperstepNo);
	// fail("GPSNode should have thrown an AssertionException because there is" +
	// " a stale incoming message to node2");
	// } catch (AssertionError e) {
	// easyMockSupport.verifyAll();
	// }
	// }
	//
	// private void runDoSuperstepComputationAndSendMessagesAndAssertMessageQueueMaps(
	// int superstepNo) {
	// GPSNode.doSuperstepComputation(superstepNo);
	// // assertOutboundMessageQueueMapsAreEqual(expectedOutboundMessageQueueMap,
	// // GPSNodeInternalGlobalVariables.outboundMessageQueueMap);
	// // assertIncomingMessageQueueMapsAreEqual(expectedIncomingMessageQueueMap,
	// // GPSNodeInternalGlobalVariables.incomingMessageQueueMap);
	// }
	//
	// private void addExpectedMessagesFromNodeId2(int superstepNo, double sum) {
	// expectedOutboundMessageQueueMap.get(machineOfNodeId4).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId4,
	// getPageRankUpdateValue(nodeId2, sum)));
	// }
	//
	// private void addExpectedMessagesFromNodeId5(int superstepNo, double sum) {
	// double pageRankUpdateValueForNode5 = getPageRankUpdateValue(nodeId5, sum);
	// expectedOutboundMessageQueueMap.get(machineOfNodeId1).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId1,
	// pageRankUpdateValueForNode5));
	// expectedOutboundMessageQueueMap.get(machineOfNodeId6).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId6,
	// pageRankUpdateValueForNode5));
	// expectedOutboundMessageQueueMap.get(machineOfNodeId4).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId4,
	// pageRankUpdateValueForNode5));
	// expectedOutboundMessageQueueMap.get(machineOfNodeId7).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId7,
	// pageRankUpdateValueForNode5));
	// expectedIncomingMessageQueueMap.get(nodeId2).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId2,
	// pageRankUpdateValueForNode5));
	// expectedIncomingMessageQueueMap.get(nodeId3).add(
	// GPSMessage.newDataMessage(superstepNo, localMachineId, nodeId3,
	// pageRankUpdateValueForNode5));
	// }
	//
	// private double getPageRankUpdateValue(int nodeId, double sum) {
	// return (0.15/graphSize + 0.85*sum) / graphPartition.get(nodeId).getNeighborIds().size();
	// }
	//
	// private static void assertMachineCommunicationHistogramMapsAreEqual(
	// Map<Integer, int[]> expectedMachineCommunicationHistogramMap,
	// Map<Integer, int[]> actualMachineCommunicationHistogramMap) {
	// assertEquals(expectedMachineCommunicationHistogramMap.size(),
	// actualMachineCommunicationHistogramMap.size());
	// for (int nodeId : expectedMachineCommunicationHistogramMap.keySet()) {
	// assertTrue(new ArrayEquals(expectedMachineCommunicationHistogramMap.get(nodeId)).matches(
	// actualMachineCommunicationHistogramMap.get(nodeId)));
	// }
	// }
	//
	// private static void assertNodesToShuffleAreEqual(Map<Integer, Byte> expectedNodesToShuffle,
	// Map<Integer, Byte> actualNodesToShuffle) {
	// assertEquals(expectedNodesToShuffle.size(), actualNodesToShuffle.size());
	// for (int nodeId : expectedNodesToShuffle.keySet()) {
	// if (expectedNodesToShuffle.get(nodeId) != actualNodesToShuffle.get(nodeId)) {
	// assertTrue(false);
	// }
	// }
	// }
	//
	// private static void assertOutboundMessageQueueMapsAreEqual(
	// Map<Byte, Queue<GPSMessage>> expectedOutboundMessageQueueMap,
	// Map<Byte, Queue<GPSMessage>> actualOutboundMessageQueueMap) {
	// assertEquals(expectedOutboundMessageQueueMap.size(), actualOutboundMessageQueueMap.size());
	// for (byte machineId: expectedOutboundMessageQueueMap.keySet()) {
	// assertTrue(actualOutboundMessageQueueMap.containsKey(machineId));
	// assertSameQueues(expectedOutboundMessageQueueMap.get(machineId),
	// actualOutboundMessageQueueMap.get(machineId));
	// }
	// }
	//
	// private static void assertIncomingMessageQueueMapsAreEqual(
	// Map<Integer, Queue<GPSMessage>> expectedIncomingMessageQueueMap,
	// Map<Integer, Queue<GPSMessage>> actualIncomingMessageQueueMap) {
	// assertEquals(expectedIncomingMessageQueueMap.size(), actualIncomingMessageQueueMap.size());
	// for (int nodeId: expectedIncomingMessageQueueMap.keySet()) {
	// assertTrue(actualIncomingMessageQueueMap.containsKey(nodeId));
	// assertSameQueues(expectedIncomingMessageQueueMap.get(nodeId),
	// actualIncomingMessageQueueMap.get(nodeId));
	// }
	// }
	//
	// private static void assertSameQueues(Queue<GPSMessage> expectedMessageQueue,
	// Queue<GPSMessage> actualMessageQueue) {
	// checkThereIsNoStaleData(actualMessageQueue);
	// assertEquals(expectedMessageQueue.size(), actualMessageQueue.size());
	// Map<int, Set<GPSMessage>> expectedGPSMessagesBySuperstep =
	// convertQueueToSuperstepNoSetOfMessagesMap(expectedMessageQueue);
	// Map<int, Set<GPSMessage>> actualGPSMessagesBySuperstep =
	// convertQueueToSuperstepNoSetOfMessagesMap(actualMessageQueue);
	// for (int superstepNo : expectedGPSMessagesBySuperstep.keySet()) {
	// assertSameGPSMessageSet(expectedGPSMessagesBySuperstep.get(superstepNo),
	// actualGPSMessagesBySuperstep.get(superstepNo));
	// }
	// }
	//
	// private static void assertSameGPSMessageSet(
	// Set<GPSMessage> expectedGPSMessages,
	// Set<GPSMessage> actualGPSMessages) {
	// for (GPSMessage expectedGPSMessage : expectedGPSMessages) {
	// GPSMessage foundActualGPSMessage = null;
	// for (GPSMessage actualGPSMessage : actualGPSMessages) {
	// if (isSameGPSMessage(expectedGPSMessage, actualGPSMessage)) {
	// foundActualGPSMessage = actualGPSMessage;
	// break;
	// }
	// }
	// assertNotNull(foundActualGPSMessage);
	// actualGPSMessages.remove(foundActualGPSMessage);
	// }
	// }
	//
	// private static Map<int, Set<GPSMessage>> convertQueueToSuperstepNoSetOfMessagesMap(
	// Queue<GPSMessage> incomingMessageQueue) {
	// Map<int, Set<GPSMessage>> superstepNoSetOfMessagesMap = new HashMap<int, Set<GPSMessage>>();
	// for (GPSMessage GPSMessage : incomingMessageQueue) {
	// int superstepNo = GPSMessage.getGPSSuperStepNo();
	// if (!superstepNoSetOfMessagesMap.containsKey(superstepNo)) {
	// superstepNoSetOfMessagesMap.put(superstepNo, new HashSet<GPSMessage>());
	// }
	// superstepNoSetOfMessagesMap.get(superstepNo).add(GPSMessage);
	// }
	// return superstepNoSetOfMessagesMap;
	// }
	//
	// private static void checkThereIsNoStaleData(
	// Queue<GPSMessage> incomingMessageQueue) {
	// if (incomingMessageQueue.isEmpty()) {
	// return;
	// }
	// int superstepNo = incomingMessageQueue.peek().getGPSSuperStepNo();
	// for (GPSMessage GPSMessage : incomingMessageQueue) {
	// assertTrue(superstepNo <= GPSMessage.getGPSSuperStepNo());
	// superstepNo = GPSMessage.getGPSSuperStepNo();
	// }
	// }
}
