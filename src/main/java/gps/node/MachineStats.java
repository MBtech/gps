package gps.node;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Helper class that is designed to hold statistics per superstep (e.g. time taken to finish a
 * superstep, number of messages to send, etc.)
 * 
 * This class is thread-safe. (Rather, I think it's thread-safe as of 08.15.2011.)
 *
 * @author semihsalihoglu
 */
public class MachineStats {

	private static Logger logger = Logger.getLogger(MachineStats.class);

	/**
	 * Custom stat keys are always assumed to be per superstep and we always take the total
	 * when exposing it through monitoring web sites.
	 * TODO(semih): Add support for taking the max and global custom stats.
	 */
	private Set<String> customStatKeys = new HashSet<String>();
	private ConcurrentHashMap<String, Double> machineDoubleStats =
		new ConcurrentHashMap<String, Double>();
	private ConcurrentHashMap<String, String> machineStringStats =
		new ConcurrentHashMap<String, String>();
		
	public void updateGlobalStat(StringStatName statName, Integer workerId, String value) {
		machineStringStats.put(getStatKey(statName.name(), null, workerId), value);
	}
	
	public String getStringGlobalStat(StringStatName statName, Integer workerId) {
		return machineStringStats.get(getStatKey(statName.name(), null, workerId));
	}

	public void updateGlobalStat(StatName statName, double value) {
		updateGlobalStat(statName.name(), null, value);
	}
	
	public void updateLatestStatus(int machineId, StatusType statusType) {
		updateGlobalStat(StatName.LATEST_STATUS.name(), machineId, (double) statusType.getId());
		updateGlobalStat(StatName.LATEST_STATUS_TIMESTAMP.name(), machineId,
			(double) System.currentTimeMillis());
	}
	
	private void updateGlobalStat(String statNameString, Integer workerId, double value) {
		machineDoubleStats.put(getStatKey(statNameString, null, workerId), value);
	}

	public void updateGlobalStat(StatName statName, Integer workerId, double value) {
		updateGlobalStat(statName.name(), workerId, value);
	}

	private void updateStatForSuperstep(String statNameString, int superstepNo, Double value) {
		updateStat(statNameString, superstepNo, value, null);
	}

	public void updateStatForSuperstep(StatName statName, int superstepNo, Double value) {
		updateStatForSuperstep(statName.name(), superstepNo, value);
	}

	public void updateStat(StatName statName, Integer superstepNo,
		Double value, Integer workerId) {
		updateStat(statName.name(), superstepNo, value, workerId);
	}

	public void updateStat(String statNameString, Integer superstepNo,
		Double value, Integer workerId) {
		if (value == null) {
			logger.error("Trying to update a stat name with null value. StatName: "
				+ statNameString);
			return;
		}
		updateStat(getStatKey(statNameString, superstepNo, workerId), value);		
	}
	
	private void updateStat(String key, double value) {
		if (machineDoubleStats.containsKey(key)) {
			machineDoubleStats.put(key, machineDoubleStats.get(key) + value);
		} else {
			machineDoubleStats.put(key, value);
		}
	}

	public boolean isTakeMaxWhenExposingStat(String strStatName) {
		if (customStatKeys.contains(strStatName)) {
			return false;
		} else {
			StatName statName = StatName.valueOf(strStatName.toUpperCase());
			return (statName != null) ? statName.isTakeMaxWhenExposing() : false;
		}
	}

	public boolean isPerSuperstepStat(String strStatName) {
		if (customStatKeys.contains(strStatName)) {
			return true;
		} else {
			StatName statName = StatName.valueOf(strStatName.toUpperCase());
			return (statName != null) ? statName.isPerSuperstep() : true;
		}
	}

	public boolean isCustomStatKey(String strStatName) {
		return customStatKeys.contains(strStatName);
	}

	public Double getStatValue(String statNameString, Integer superstepNo, Integer workerId) {
		return machineDoubleStats.get(getStatKey(statNameString, superstepNo, workerId));
	}

	public Double getStatValue(StatName statName, int superstepNo) {
		return getStatValue(statName.name(), superstepNo);
	}
	
	public Double getStatValue(String statNameString, int superstepNo) {
		return getStatValue(statNameString, superstepNo, null);
	}

	public Double getStatValue(StatName statName) {
		return getStatValue(statName.name());
	}

	public Double getStatValue(String statNameString) {
		return machineDoubleStats.get(statNameString);
	}

	private String getStatKey(String statNameString, Integer superstepNo, Integer workerId) {
		if (workerId == null) {
			if (superstepNo == null) {
				return statNameString;
			} else {
				return statNameString + "-" + superstepNo;
			}
		} else if (superstepNo == null) {
			return workerId + "-" + statNameString;
		} else {
			return workerId + "-" + statNameString + "-" + superstepNo;
		}
	}

	public void logStatsForSuperstepForAllWorkers(int superstepNo, MachineConfig machineConfig) {
		logStatsForSuperstep(superstepNo, Utils.MASTER_ID, false /* is master */);
		for (int workerId : machineConfig.getWorkerIds()) {
			logStatsForSuperstep(superstepNo, workerId, false /* is master */);
		}
	}

	public void logStatsForSuperstep(int superstepNo, Integer workerId, boolean isWorker) {
		logger.info(Utils.getStatsLoggingHeader("Machine stats for worker " + workerId
			+ " superstepNo: " + superstepNo));
		for (StatName statName : StatName.values()) {
			if (statName.isPerSuperstep) {
				logger.info("StatName: " + statName.name() + " StatValue: "
					+ getStatValue(statName.name(), superstepNo, isWorker ? null : workerId));
			}
		}
		logger.info(Utils.getStatsLoggingHeader("End of machine stats for superstepNo: "
			+ superstepNo));
	}

	public void logGlobalStats() {
		logger.info(Utils.LOGGER_HEADER_SECOND_PART + " Global machine stats"
			+ Utils.LOGGER_HEADER_SECOND_PART);
		for (StatName statName : StatName.values()) {
			if (!statName.isPerSuperstep) {
				logger.info("StatName: " + statName.name() + " StatValue: "
					+ getStatValue(statName.name()));
			}
		}
		logger.info(Utils.LOGGER_HEADER_FIRST_PART + " End of global machine stats"
			+ Utils.LOGGER_HEADER_SECOND_PART);
	}

	public Set<String> getCustomStatKeys() {
		return customStatKeys;
	}

	public void addCustomStatKey(String key) {
		customStatKeys.add(key);
	}

	public void putDoubleStat(String key, Double value) {
		machineDoubleStats.put(key, value);
	}

	public void putStringStat(String key, String value) {
		machineStringStats.put(key, value);
	}

	public Map<String, Double> getMachineDoubleStats() {
		return machineDoubleStats;
	}
	
	public Map<String, String> getMachineStringStats() {
		return machineStringStats;
	}
	
	public Set<String> getMachineDoubleStatsKeys() {
		return machineDoubleStats.keySet();
	}

	public Set<String> getMachineStringStatsKeys() {
		return machineStringStats.keySet();
	}

	public enum StringStatName {
		EXCEPTION_STACK_TRACE(1);
		private static Map<Integer, StringStatName> idStatNameMap =
			new HashMap<Integer, StringStatName>();
		static {
			for (StringStatName statName : StringStatName.values()) {
				idStatNameMap.put(statName.id, statName);
			}
		}

		private int id;

		private StringStatName(int id) {
			this.id = id;
		}
		
		public int getId() {
			return id;
		}
		
		public static StringStatName getStatNameFromId(int value) {
			return idStatNameMap.get(value);
		}
	}
	
	public enum StatName {
		NUM_VERTICES(1, true, false),
		NUM_EDGES(2, true, false) ,
		EDGE_DENSITY(3, true, true),
		TOTAL_DO_SUPERSTEP_COMPUTATION_TIME(7, true, true),
		TOTAL_TIME(11, true, true),
		TOTAL_TIME_UNTIL_RECEIVING_ALL_FINAL_DATA_SENT_MESSAGES(9, true, true),
		TOTAL_TIME_UNTIL_SENDING_ALL_FINAL_DATA_SENT_MESSAGES(10, true, true),
		WAIT_TIME_FOR_END_OF_SUPERSTEP_CONTROL_MESSAGES_TO_BE_RECEIVED(12, true, true),
		WAIT_TIME_FOR_FINAL_DATA_SENT_MESSAGES_CONTROL_MESSAGES_TO_BE_SENT(19, true, true),
		WAIT_TIME_FOR_FINAL_DATA_SENT_MESSAGES_CONTROL_MESSAGES_TO_BE_RECEIVED(20, true, true),
		WAIT_TIME_FOR_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE_CONTROL_MESSAGES_TO_BE_SENT(21, true, true),
		WAIT_TIME_FOR_BEGIN_SUPERSTEP_OR_TERMINATE_CONTROL_MESSAGE_TO_BE_RECEIVED(22, true, true),
		TOTAL_BYTES_SENT(23, true, false),
		TOTAL_BYTES_RECEIVED(24, true, false),
		TOTAL_MESSAGES_SENT(25, true, false),
		START_TIME(0, false, true),
		SUPERSTEP_START_TIME(4, true, true),
		END_TIME_BEFORE_WRITING_OUTPUT(5, false, true),
		END_TIME_AFTER_WRITING_OUTPUT(6, false, true),
		WAIT_TIME_FOR_TERMINATION_CONTROL_MESSAGES_TO_BE_SENT(13, false, true),
		WAIT_TIME_FOR_TERMINATION_CONTROL_MESSAGES_TO_BE_RECEIVED(14, false, true),
		WAIT_TIME_FOR_VERTEX_SHUFFLING_CONTROL_MESSAGES_TO_BE_SENT(15, true, true),
		WAIT_TIME_FOR_VERTEX_SHUFFLING_CONTROL_MESSAGES_TO_BE_RECEIVED(16, true, true),
		WAIT_TIME_FOR_FINISHED_PARSING_VERTEX_SHUFFLING_CONTROL_MESSAGES_TO_BE_SENT(17, true, true),
		WAIT_TIME_FOR_FINISHED_PARSING_VERTEX_SHUFFLING_CONTROL_MESSAGES_TO_BE_RECEIVED(18, true, true),
		NUM_VERTICES_SENT(26, true, false),
		NUM_VERTICES_RECEIVED(27, true, false),
		NUM_ACTIVE_NODES_FOR_THIS_SUPERSTEP(28, true, false),
		NUM_ACTIVE_NODES_FOR_NEXT_SUPERSTEP(29, true, false),
		NUM_PREVIOUSLY_ACTIVE_NODES(30, true, false),
		NUM_NODES_MADE_ACTIVE_BY_INCOMING_MESSAGES(31, true, false),
		TOTAL_TIME_SPENT_ON_SENDING_VERTICES_AND_REMOVING_THEM_FROM_GRAPH_PARTITION(32, true, true),
		TOTAL_TIME_SPENT_ON_SENDING_EXCEPTION_FILES_TO_ALL_MACHINES(33, true, true),
		TOTAL_TIME_BEFORE_WRITING_OUTPUT(34, false, true),
		TOTAL_TIME_AFTER_WRITING_OUTPUT(35, false, true),
		DATA_PARSER_FIRST_MESSAGE_PARSING_TIME(36, true, true),
		DATA_PARSER_LAST_MESSAGE_PARSING_TIME(37, true, true),
		DATA_PARSER_TIME_SPENT_ON_ADD_MESSAGE_TO_QUEUES(40, true, true),
		VERTEX_SHUFFLING_MESSAGE_PARSER_TIME_SPENT_ON_WAITING_FOR_BUFFERS_TO_ARRIVE(41, true, true),
		LATEST_STATUS(42, false, true),
		LATEST_STATUS_TIMESTAMP(43, false, true),
		SYSTEM_START_TIME(44, false, true),
		WAIT_TIME_FOR_FINISHED_SENDING_VERTEX_STATE_DATA_CONTROL_MESSAGE_TO_BE_RECEIVED(45, true, true),
		TOTAL_MESSAGES_RECEIVED(48, true, false),
		NUM_EXCEPTION_VERTICES_RECEIVED(49, true, false),
		NUM_VERTEX_DATA_RECEIVED(50, true, false),
		NUM_VERTICES_SENT_IN_PREVIOUS_SUPERSTEP(51, true, false),
		NUM_VERTICES_RECEIVED_IN_PREVIOUS_SUPERSTEP(52, true, false),
		IDS_MAP_SIZE(54, true, false),
		TOTAL_DATA_BYTES_RECEIVED(56, true, false),
		TOTAL_DATA_BYTES_SENT(57, true, false),
		NUM_HIGH_BENEFIT_VERTICES_SENT(58, true, false),
		CONNECTION_ESTABLISHMENT_TIMESTAMP(59, false, true),
		TOTAL_SLEEP_TIME_FOR_OUTGOING_BUFFERS(60, true, true),
		TOTAL_DO_EXTRA_WORK_BEFORE_SUPERSTEP_COMPUTATION_TIME(61, true, true),
		TOTAL_DO_EXTRA_WORK_AFTER_RECEIVING_ALL_FINAL_DATA_MESSAGES_TIME(62, true, true),
		TOTAL_PARSING_VERTICES_RECEIVED_TIME(63, true, true),
		TOTAL_PARSING_EXCEPTION_MESSAGES_TIME(64, true, true),
		TOTAL_RELABELING_TIME(65, true, true),
		WAIT_TIME_FOR_GLOBAL_OBJECTS_MESSAGES_TO_BE_RECEIVED(66, true, true),
		WAIT_TIME_FOR_LARGE_VERTEX_PARTITIONING_MESSAGES_TO_BE_RECEIVED(66, false, true),
		DATA_PARSER_TIME_SPENT_ON_LARGE_VERTEX_PARTITONS_MESSAGES(67, true, true),
		WAIT_TIME_FOR_READY_TO_START_COMPUTATION(68, false, true),
		DATA_PARSER_TIME_SPENT_ON_LARGE_VERTEX_DATA_MESSAGES(69, true, true),
		WAIT_TIME_FOR_FINAL_INITIAL_VERTEX_PARTITIONING_MESSAGES_TO_BE_RECEIVED(70, false, true),
		NUM_LARGE_VERTICES_PARTITIONED(71, true, false),
		TOTAL_NETWORK_MESSAGE_SENDING_TIME(72, true, true),
		TOTAL_DECODER_STATE_TIME(73, true, true),
		TOTAL_TIME_SPENT_ON_LARGE_VERTEX_COMPUTATION(74, true, true);

		private static Map<Integer, StatName> idStatNameMap = new HashMap<Integer, StatName>();
		private static Map<String, StatName> statNameStringStatNameMap = new HashMap<String, StatName>();
		static {
			for (StatName statName : StatName.values()) {
				idStatNameMap.put(statName.id, statName);
				statNameStringStatNameMap.put(statName.name(), statName);
			}
		}

		private int id;
		private boolean isPerSuperstep;
		private final boolean takeMaxWhenExposing;

		private StatName(int id, boolean isPerSuperstep, boolean takeMaxWhenExposing) {
			this.id = id;
			this.isPerSuperstep = isPerSuperstep;
			this.takeMaxWhenExposing = takeMaxWhenExposing;
		}
		
		public boolean isPerSuperstep() {
			return isPerSuperstep;
		}
		
		public int getId() {
			return id;
		}
		
		public boolean isTakeMaxWhenExposing() {
			return takeMaxWhenExposing;
		}

		public static boolean isDefaultStatName(String statNameString) {
			return statNameStringStatNameMap.containsKey(statNameString);
		}

		public static StatName getStatNameFromName(String stringName) {
			return statNameStringStatNameMap.get(stringName);
		}

		public static StatName getStatNameFromId(int value) {
			return idStatNameMap.get(value);
		}
	}
}
