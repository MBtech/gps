package gps.globalobjects;

import gps.writable.FloatWritable;
import gps.writable.MinaWritable;

/**
 * Please read {@link BooleanOverwriteGlobalObject} for explanations as to how overwrite global
 * objects work.
 */
public class FloatOverwriteGlobalObject extends FloatGlobalObject {

	public FloatOverwriteGlobalObject() {
		super();
	}

	public FloatOverwriteGlobalObject(float value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue((FloatWritable) otherValue);
	}
}