package gps.globalobjects;

import gps.writable.FloatWritable;
import gps.writable.MinaWritable;

public class FloatSumGlobalObject extends FloatGlobalObject {

	public FloatSumGlobalObject() {
		super();
	}

	public FloatSumGlobalObject(float value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new FloatWritable(getValue().getValue() +
			((FloatWritable) otherValue).getValue()));
	}
}
