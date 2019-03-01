package gps.globalobjects;

import gps.writable.LongWritable;
import gps.writable.MinaWritable;

/**
 * Please read {@link BooleanOverwriteGlobalObject} for explanations as to how overwrite global
 * objects work.
 */
public class LongOverwriteGlobalObject extends LongGlobalObject {

	public LongOverwriteGlobalObject() {
		super();
	}

	public LongOverwriteGlobalObject(long value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue((LongWritable) otherValue);
	}
}