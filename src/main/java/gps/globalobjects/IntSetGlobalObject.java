package gps.globalobjects;

import gps.writable.IntSetWritable;
import gps.writable.MinaWritable;

public class IntSetGlobalObject extends GlobalObject<IntSetWritable> {

	public IntSetGlobalObject() {
		setValue(new IntSetWritable());
	}

	public IntSetGlobalObject(IntSetWritable value) {
		setValue(value);
	}

 	@Override
	public void update(MinaWritable minaWritable) {
		getValue().value.addAll(((IntSetWritable) minaWritable).value);
	}
}