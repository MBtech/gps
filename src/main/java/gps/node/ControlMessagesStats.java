package gps.node;


import gps.node.MachineStats.StatName;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Helper class designed to maintain which machines sent which control messages.
 * 
 * TODO(semih): Make sure that this class is thread-safe. I think it is, but go over it again to
 * verify. WARNING: Make sure that when you edit this class that anything code that modifies the
 * underlying maps/lists is synchronized. So should any code that loops over the data.
 * 
 * @author semihsalihoglu
 */
public class ControlMessagesStats {

	private static Logger logger = Logger.getLogger(ControlMessagesStats.class);

	private volatile Map<Integer, Map<ControlMessageType, List<ControlMessage>>> perSupestepControlMessageMaps =
		new ConcurrentHashMap<Integer, Map<ControlMessageType, List<ControlMessage>>>();

	private Map<ControlMessageType, List<ControlMessage>> globalControlMessageMap =
		new ConcurrentHashMap<ControlMessageType, List<ControlMessage>>();

	public synchronized void addControlMessage(int superstepNo, ControlMessageType controlMessageType,
		int fromOrToMachineId, boolean booleanValue, int integerValue) {
		ControlMessage controlMessage =
			new ControlMessage(fromOrToMachineId);
		controlMessage.setBooleanValue(booleanValue);
		controlMessage.setIntegerValue(integerValue);
		addPerSuperstepControlMessage(superstepNo, controlMessageType, controlMessage);	
	}
	
	public synchronized void addBooleanValueControlMessage(int superstepNo,
		ControlMessageType controlMessageType, int fromOrToMachineId, boolean booleanValue) {
		ControlMessage controlMessage = new ControlMessage(fromOrToMachineId);
		controlMessage.setBooleanValue(booleanValue);
		addPerSuperstepControlMessage(superstepNo, controlMessageType, controlMessage);
	}

	public synchronized void addGlobalControlMessage(ControlMessageType controlMessageType,
		int fromOrToMachineId) {
		addGlobalControlMessage(controlMessageType, new ControlMessage(fromOrToMachineId));
	}

	private synchronized void addGlobalControlMessage(ControlMessageType controlMessageType,
		ControlMessage controlMessage) {
		synchronized (this) {
			addListIfNotExists(globalControlMessageMap, controlMessageType);
			globalControlMessageMap.get(controlMessageType).add(controlMessage);
		}
	}

	public synchronized void addPerSuperstepControlMessage(int superstepNo,
		ControlMessageType controlMessageType, int fromOrToMachineId) {
		addPerSuperstepControlMessage(superstepNo, controlMessageType, new ControlMessage(fromOrToMachineId));
	}

	public synchronized boolean isAllControlMessagesFalse(int superstepNo,
		ControlMessageType controlMessageType) {
		synchronized (this) {
			List<ControlMessage> controlMessageList =
				perSupestepControlMessageMaps.get(superstepNo).get(controlMessageType);
			for (ControlMessage controlMessage : controlMessageList) {
				if (controlMessage.booleanValue != null && controlMessage.booleanValue == true) {
					return false;
				}
			}
			return true;
		}
	}

	public synchronized boolean getControlMessageBooleanValue(int superstepNo,
		ControlMessageType controlMessageType) {
		synchronized (this) {
			return perSupestepControlMessageMaps.get(superstepNo).get(controlMessageType).get(0)
				.booleanValue;
		}
	}

	public synchronized int getControlMessageIntegerValue(int superstepNo,
		ControlMessageType controlMessageType) {
		synchronized (this) {
			return perSupestepControlMessageMaps.get(superstepNo).get(controlMessageType).get(0)
				.integerValue;
		}
	}

	public synchronized boolean hasReceivedAllGlobalControlMessages(ControlMessageType controlMessageType,
		int expectedControlMessages) {
		synchronized (this) {
			return globalControlMessageMap.get(controlMessageType).size() == expectedControlMessages;
		}
	}

	public synchronized boolean hasReceivedAllPerSuperstepControlMessages(int superstepNo,
		ControlMessageType controlMessageType, int expectedControlMessages) {
		synchronized (this) {
			return perSupestepControlMessageMaps.get(superstepNo).get(controlMessageType).size() == expectedControlMessages;
		}
	}

	public synchronized void logGlobalControlMessages(ControlMessageType controlMessageType) {
		synchronized (this) {
			addListIfNotExists(globalControlMessageMap, controlMessageType);
		}
	}

	public synchronized void logPerSuperstepControlMessages(int superstepNo,
		ControlMessageType controlMessageType) {
		synchronized (this) {
			addMapAndListIfNotExists(superstepNo, controlMessageType);
		}
	}

	private synchronized void addPerSuperstepControlMessage(int superstepNo,
		ControlMessageType controlMessageType, ControlMessage controlMessage) {
		synchronized (this) {
			addMapAndListIfNotExists(superstepNo, controlMessageType);
			logger.debug("Adding a control message of type: " + controlMessageType.name()
				+ " superstepNo: " + superstepNo);
			perSupestepControlMessageMaps.get(superstepNo).get(controlMessageType)
				.add(controlMessage);
		}
	}

	private synchronized void addMapAndListIfNotExists(int superstepNo,
		ControlMessageType controlMessageType) {
		if (!perSupestepControlMessageMaps.containsKey(superstepNo)) {
			perSupestepControlMessageMaps.put(superstepNo,
				new HashMap<ControlMessageType, List<ControlMessage>>());
		}
		addListIfNotExists(perSupestepControlMessageMaps.get(superstepNo), controlMessageType);
	}

	private synchronized void addListIfNotExists(
		Map<ControlMessageType, List<ControlMessage>> controlMessagesMap,
		ControlMessageType controlMessageType) {
		if (!controlMessagesMap.containsKey(controlMessageType)) {
			controlMessagesMap.put(controlMessageType, new LinkedList<ControlMessage>());
		}
	}

	/**
	 * Helper class to hold information (its arrival time, which machine it's from, etc.) about a
	 * particular control message.
	 */
	public static class ControlMessage {
		private int fromOrToMachineId;
		private Boolean booleanValue = null;
		private Integer integerValue = null;

		private ControlMessage(int fromOrToMachineId) {
			this.fromOrToMachineId = fromOrToMachineId;
		}

		private void setBooleanValue(boolean booleanValue) {
			this.booleanValue = booleanValue;
		}

		private void setIntegerValue(int integerValue) {
			this.integerValue = integerValue;
		}

		public int getFromOrToMachineId() {
			return fromOrToMachineId;
		}
	}

	public enum ControlMessageType {
		RECEIVED_END_OF_SUPERSTEP_MESSAGES(
			StatName.WAIT_TIME_FOR_END_OF_SUPERSTEP_CONTROL_MESSAGES_TO_BE_RECEIVED),
		SENT_FINAL_DATA_SENT_MESSAGES(
			StatName.WAIT_TIME_FOR_FINAL_DATA_SENT_MESSAGES_CONTROL_MESSAGES_TO_BE_SENT),
		RECEIVED_FINAL_DATA_SENT_MESSAGES(
			StatName.WAIT_TIME_FOR_FINAL_DATA_SENT_MESSAGES_CONTROL_MESSAGES_TO_BE_RECEIVED),
		SENT_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE(
			StatName.WAIT_TIME_FOR_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE_CONTROL_MESSAGES_TO_BE_SENT),
		RECEIVED_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE_MESSAGE(
			StatName.WAIT_TIME_FOR_BEGIN_SUPERSTEP_OR_TERMINATE_CONTROL_MESSAGE_TO_BE_RECEIVED),
		RECEIVED_GLOBAL_OBJECTS_MESSAGES(
			StatName.WAIT_TIME_FOR_GLOBAL_OBJECTS_MESSAGES_TO_BE_RECEIVED),
		RECEIVED_LARGE_VERTEX_PARTITIONING_MESSAGES(
			StatName.WAIT_TIME_FOR_LARGE_VERTEX_PARTITIONING_MESSAGES_TO_BE_RECEIVED),
		READY_TO_START_COMPUTATION(StatName.WAIT_TIME_FOR_READY_TO_START_COMPUTATION),
		RECEIVED_FINAL_INITIAL_VERTEX_PARTITIONING_MESSAGES(
			StatName.WAIT_TIME_FOR_FINAL_INITIAL_VERTEX_PARTITIONING_MESSAGES_TO_BE_RECEIVED),;

		private StatName statName;

		private ControlMessageType(StatName statName) {
			this.statName = statName;
		}

		public StatName getStatName() {
			return statName;
		}
	}
}
