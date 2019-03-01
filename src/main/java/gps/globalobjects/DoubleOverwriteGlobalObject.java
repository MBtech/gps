package gps.globalobjects;

import gps.writable.DoubleWritable;
import gps.writable.MinaWritable;

/**
 * Please read {@link BooleanOverwriteGlobalObject} for explanations as to how overwrite global
 * objects work.
 */
public class DoubleOverwriteGlobalObject extends DoubleGlobalObject {

	public DoubleOverwriteGlobalObject() {
		super();
	}

	public DoubleOverwriteGlobalObject(double value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue((DoubleWritable) otherValue);
	}
}
