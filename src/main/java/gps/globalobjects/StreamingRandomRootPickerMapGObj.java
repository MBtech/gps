package gps.globalobjects;

import java.util.Map;
import java.util.Map.Entry;

import gps.writable.MinaWritable;
import gps.writable.StreamingRandomRootPickerMapWritable;
import gps.writable.StreamingRootPickerValue;

/**
 * Picks a random integer from a stream of integers. It contains a map where keys consist of one integer
 * and two bytes and values consist of one integer and an integer array. For each key a set of random integers
 * are picked.
 * Each key identifies one possible component:
 * <ul>
 * 	<li> compId: integer - id of a vertex from the component
 * 	<li> fwId: byte-1 - id of the min vertex reached in the forward traversal
 * 	<li> bwId: byte-2 - id of the min vertex reached in the backward traversal from the root
 * </ul>
 * Each value consists of:
 * <ul>
 * 	<li> numInserted: the number of integers inserted with that key
 * 	<li> k values, each picked with prob k/numInserted
 * </ul>
 * 
 * The update function takes a map and merges the value of each key by maintaining that each k value is
 * picked with probability k/numInserted1+numInserted2.
 *
 * @author semihsalihoglu
 */
public class StreamingRandomRootPickerMapGObj extends GlobalObject<StreamingRandomRootPickerMapWritable> {

	public StreamingRandomRootPickerMapGObj() {
		setValue(new StreamingRandomRootPickerMapWritable());
	}

	public StreamingRandomRootPickerMapGObj(Map<Integer, StreamingRootPickerValue> componentRootsMap,
		int k) {
		setValue(new StreamingRandomRootPickerMapWritable(componentRootsMap, k));
	}

	@Override
	public void update(MinaWritable otherWritable) {
		System.out.println("Updating StreamingRandomRootPickerGO with another writable.");
		StreamingRandomRootPickerMapWritable otherMap = (StreamingRandomRootPickerMapWritable) otherWritable;
		for (Entry<Integer, StreamingRootPickerValue> keyValue : otherMap.keyValueMap.entrySet()) {
			StreamingRootPickerValue value = getValue().keyValueMap.get(keyValue.getKey());
			System.out.println("value: " + value);
			if (value == null) {
				getValue().keyValueMap.put(keyValue.getKey(), keyValue.getValue());
			} else {
				value.insertNewIntValues(keyValue.getValue());
			}
		}
	}
}
