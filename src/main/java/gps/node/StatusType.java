package gps.node;

import java.util.HashMap;
import java.util.Map;

public enum StatusType {
	// For Workers
	DOING_COMPUTATION(0),
	WAITING_FOR_FINAL_DATA_MESSAGES_TO_BE_SENT(1),
	WAITING_FOR_FINAL_DATA_MESSAGES_TO_BE_RECEIVED(2),
	END_OF_SUPERSTEP(3),
	DOING_LARGE_VERTEX_PARTITIONING(10),
	WAITING_FOR_LARGE_VERTEX_PARTITIONING_MESSAGES_TO_BE_RECEIVED(11),
	READY_TO_DO_COMPUTATION(12),
	DOING_INITIAL_VERTEX_PARTITIONING(13),

	// For Master
	STARTING_UP(4),
	ESTABLISHING_TCP_CONNECTIONS(5),
	WAITING_FOR_END_OF_SUPERSTEP_MESSAGES_FROM_WORKERS(6),
	WAITING_FOR_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE_MESSAGES_TO_BE_SENT_TO_WORKERS(7),
	EXCEPTION_THROWN(8),
	DOING_EXTRA_WORK_AFTER_RECEIVING_FINAL_DATA_MESSAGES(9);

	private static Map<Integer, StatusType> idTypeMap = new HashMap<Integer, StatusType>();
	static {
		for (StatusType type : StatusType.values()) {
			idTypeMap.put(type.id, type);
		}
	}

	private int id;

	private StatusType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static StatusType getTypeFromId(int id) {
		return idTypeMap.get(id);
	}

}
