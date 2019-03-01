package gps.globalobjects;

import gps.writable.FloatWritable;
import gps.writable.MinaWritable;

public class FloatMinGlobalObject extends FloatGlobalObject {

	public FloatMinGlobalObject() {
		super();
	}

	public FloatMinGlobalObject(float value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new FloatWritable(Math.min(getValue().getValue(),
			((FloatWritable) otherValue).getValue())));
	}
}
