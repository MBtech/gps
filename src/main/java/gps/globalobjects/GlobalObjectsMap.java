package gps.globalobjects;

import gps.writable.MinaWritable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GlobalObjectsMap {

	public static String NUM_VERTICES = "num-vertices";
	public static String NUM_EDGES = "num-edges";
	public static String NUM_ACTIVE_VERTICES = "num-active-vertices";
	public static String NUM_TOTAL_VERTICES = "num-total-vertices";
	public static String NUM_TOTAL_EDGES = "num-total-edges";
	public static String NUM_TOTAL_ACTIVE_VERTICES = "num-total-active-vertices";
	
	private static Set<String> defaultGOKeys = new HashSet<String>();
	
	static {
		defaultGOKeys.add(NUM_VERTICES);
		defaultGOKeys.add(NUM_ACTIVE_VERTICES);
		defaultGOKeys.add(NUM_EDGES);
		defaultGOKeys.add(NUM_TOTAL_VERTICES);
		defaultGOKeys.add(NUM_TOTAL_EDGES);
		defaultGOKeys.add(NUM_TOTAL_ACTIVE_VERTICES);
	}

	private Map<String, GlobalObject<? extends MinaWritable>> globalObjectsMap =
		new HashMap<String, GlobalObject<? extends MinaWritable>>();

	public GlobalObject<? extends MinaWritable> getGlobalObject(String key) {
		return globalObjectsMap.get(key);
	}

	public void putGlobalObject(String key, GlobalObject<? extends MinaWritable> bv) {
		globalObjectsMap.put(key, bv);
	}
	
	public void removeGlobalObject(String key) {
		globalObjectsMap.remove(key);
	}

	public void clearNonDefaultObjects() {
		Set<String> keySet = new HashSet<String>(globalObjectsMap.keySet());
		for (String key : keySet) {
			if (!defaultGOKeys.contains(key)) {
				globalObjectsMap.remove(key);
			}
		}
	}

	public Set<String> keySet() {
		return globalObjectsMap.keySet();
	}

	public void putOrUpdateGlobalObject(String key,
		GlobalObject<? extends MinaWritable> bv) {
		if (!globalObjectsMap.containsKey(key)) {
			putGlobalObject(key, bv);
		} else {
			getGlobalObject(key).update(bv.getValue());
		}
	}
}