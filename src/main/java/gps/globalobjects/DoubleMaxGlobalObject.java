package gps.globalobjects;

import gps.writable.DoubleWritable;
import gps.writable.MinaWritable;

public class DoubleMaxGlobalObject extends DoubleGlobalObject {

	public DoubleMaxGlobalObject() {
		super();
	}

	public DoubleMaxGlobalObject(double value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new DoubleWritable(Math.max(getValue().getValue(),
			((DoubleWritable) otherValue).getValue())));
	}
}
