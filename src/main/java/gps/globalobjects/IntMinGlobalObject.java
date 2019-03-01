package gps.globalobjects;

import gps.writable.IntWritable;
import gps.writable.MinaWritable;

public class IntMinGlobalObject extends IntGlobalObject {

	public IntMinGlobalObject() {
		super();
	}

	public IntMinGlobalObject(int value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new IntWritable(Math.min(getValue().getValue(),
			((IntWritable) otherValue).getValue())));
	}
}