package gps.messages;

import java.util.HashMap;
import java.util.Map;

public enum MessageTypes {
	DATA(0),
	MACHINE_ID_INFORMATION(2),
	VERTEX_SHUFFLING_WITH_DATA(8), // For greedy dynamic partitioners
	EXCEPTIONS_MAP(9), // For greedy dynamic partitioners
	FINAL_DATA_SENT(11),
	FINISHED_PARSING_DATA_MESSAGES(12),
	BEGIN_NEXT_SUPERSTEP_OR_TERMINATE(13),
	STATUS_UPDATE(14),
	POTENTIAL_NUM_VERTICES_TO_SEND(15),
	GLOBAL_OBJECTS(16),
	LARGE_VERTEX_PARTITIONS(17),
	LARGE_VERTEX_DATA(18),
	INPUT_SPLIT(19),
	INITIAL_VERTEX_PARTITIONING(20),
	FINAL_INITIAL_VERTEX_PARTITIONING_SENT(21);

	private static Map<Integer, MessageTypes> idTypeMap = new HashMap<Integer, MessageTypes>();
	static {
		for (MessageTypes type : MessageTypes.values()) {
			idTypeMap.put(type.id, type);
		}
	}

	private int id;

	private MessageTypes(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static MessageTypes getTypeFromId(int id) {
		return idTypeMap.get(id);
	}

}