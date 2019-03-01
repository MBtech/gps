package gps.writable;

import gps.globalobjects.GraphWritable;

import java.util.List;

public class NullValueGraphWritable extends GraphWritable {
	
	public NullValueGraphWritable() {
		super();
	}
	
	public NullValueGraphWritable(List<NodeWritable> nodes) {
		super(nodes);
	}
}