package gps.globalobjects;

import gps.writable.DoubleWritable;
import gps.writable.MinaWritable;

public class DoubleSumGlobalObject extends DoubleGlobalObject {

	public DoubleSumGlobalObject() {
		super();
	}

	public DoubleSumGlobalObject(double value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new DoubleWritable(getValue().getValue() +
			((DoubleWritable) otherValue).getValue()));
	}
}
