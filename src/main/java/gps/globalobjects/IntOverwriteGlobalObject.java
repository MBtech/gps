package gps.globalobjects;

import gps.writable.IntWritable;
import gps.writable.MinaWritable;

/**
 * Please read {@link BooleanOverwriteGlobalObject} for explanations as to how overwrite global
 * objects work.
 */
public class IntOverwriteGlobalObject extends IntGlobalObject {

	public IntOverwriteGlobalObject() {
		super();
	}

	public IntOverwriteGlobalObject(int value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue((IntWritable) otherValue);
	}
}