package gps.globalobjects;

import gps.writable.IntWritable;
import gps.writable.MinaWritable;

public class IntProductGlobalObject extends IntGlobalObject {

	public IntProductGlobalObject() {
		super();
	}

	public IntProductGlobalObject(int value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new IntWritable(getValue().getValue() * ((IntWritable) otherValue).getValue()));
	}
}