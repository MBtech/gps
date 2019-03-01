package gps.globalobjects;

import gps.writable.LongWritable;
import gps.writable.MinaWritable;

public class LongMinGlobalObject extends LongGlobalObject {

	public LongMinGlobalObject() {
		super();
	}

	public LongMinGlobalObject(Long value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new LongWritable(Math.min(getValue().getValue(),
			((LongWritable) otherValue).getValue())));
	}
}