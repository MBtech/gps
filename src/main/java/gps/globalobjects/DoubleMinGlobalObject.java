package gps.globalobjects;

import gps.writable.DoubleWritable;
import gps.writable.MinaWritable;

public class DoubleMinGlobalObject extends DoubleGlobalObject {

	public DoubleMinGlobalObject() {
		super();
	}

	public DoubleMinGlobalObject(double value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new DoubleWritable(Math.min(getValue().getValue(),
			((DoubleWritable) otherValue).getValue())));
	}
}
