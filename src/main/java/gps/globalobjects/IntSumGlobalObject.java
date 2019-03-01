package gps.globalobjects;

import gps.writable.IntWritable;
import gps.writable.MinaWritable;

public class IntSumGlobalObject extends IntGlobalObject {

	public IntSumGlobalObject() {
		super();
	}

	public IntSumGlobalObject(int value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new IntWritable(getValue().getValue() + ((IntWritable) otherValue).getValue()));
	}
}