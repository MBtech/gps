package gps.globalobjects;

import gps.writable.IntWritable;
import gps.writable.MinaWritable;

public class IntMaxGlobalObject extends IntGlobalObject {

	public IntMaxGlobalObject() {
		super();
	}

	public IntMaxGlobalObject(int value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new IntWritable(Math.max(getValue().getValue(),
			((IntWritable) otherValue).getValue())));
	}
}