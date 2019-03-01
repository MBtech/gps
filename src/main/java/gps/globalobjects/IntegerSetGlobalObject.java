package gps.globalobjects;

import java.util.HashSet;
import java.util.Set;

import gps.writable.IntSetWritable;
import gps.writable.MinaWritable;

public class IntegerSetGlobalObject extends GlobalObject<IntSetWritable> {

	public IntegerSetGlobalObject() {
		setValue(new IntSetWritable(new HashSet<Integer>()));
	}

	public IntegerSetGlobalObject(Set<Integer> intSet) {
		setValue(new IntSetWritable(intSet));
	}

	@Override
	public void update(MinaWritable minaWritable) {
		IntSetWritable otherWritable = (IntSetWritable) minaWritable;
		Set<Integer> thisIntSet = getValue().value;
		for (Integer otherSetIntValue : otherWritable.value) {
			thisIntSet.add(otherSetIntValue);
		}
	}
}