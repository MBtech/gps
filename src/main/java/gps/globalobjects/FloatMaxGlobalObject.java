package gps.globalobjects;

import gps.writable.FloatWritable;
import gps.writable.MinaWritable;

public class FloatMaxGlobalObject extends FloatGlobalObject {

	public FloatMaxGlobalObject() {
		super();
	}

	public FloatMaxGlobalObject(float value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new FloatWritable(Math.max(getValue().getValue(),
			((FloatWritable) otherValue).getValue())));
	}
}
