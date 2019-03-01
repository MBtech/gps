package gps.globalobjects;

import gps.writable.MinaWritable;
import gps.writable.StreamingRootPickerWritable;

public class StreamingRootPickerGObj extends GlobalObject<StreamingRootPickerWritable>{

	public StreamingRootPickerGObj() {
		setValue(new StreamingRootPickerWritable());
	}
	
	public StreamingRootPickerGObj(StreamingRootPickerWritable rootPickerWritable) {
		setValue(rootPickerWritable);
	}

	@Override
	public void update(MinaWritable otherWritable) {
		getValue().rootPicker.insertNewIntValues(((StreamingRootPickerWritable) otherWritable).rootPicker);
	}
}
