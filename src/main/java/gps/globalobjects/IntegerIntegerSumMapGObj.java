package gps.globalobjects;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import gps.writable.IntegerIntegerMapWritable;
import gps.writable.MinaWritable;

public class IntegerIntegerSumMapGObj extends GlobalObject<IntegerIntegerMapWritable> {

	public IntegerIntegerSumMapGObj() {
		setValue(new IntegerIntegerMapWritable(new HashMap<Integer, Integer>()));
	}

	public IntegerIntegerSumMapGObj(Map<Integer, Integer> randomRootPickerMap) {
		setValue(new IntegerIntegerMapWritable(randomRootPickerMap));
	}

	@Override
	public void update(MinaWritable minaWritable) {
		IntegerIntegerMapWritable otherWritable = (IntegerIntegerMapWritable) minaWritable;
		Map<Integer, Integer> thisComponentKeyIntegerMap = getValue().integerIntegerMap;
		for (Entry<Integer, Integer> otherWritableEntry : otherWritable.integerIntegerMap.entrySet()) {
			int otherKey = otherWritableEntry.getKey();
			if (thisComponentKeyIntegerMap.containsKey(otherKey)) {
				thisComponentKeyIntegerMap.put(otherKey,
					thisComponentKeyIntegerMap.get(otherKey) + otherWritableEntry.getValue());
			} else {
				thisComponentKeyIntegerMap.put(otherKey, otherWritableEntry.getValue());
			}
		}
	}
}