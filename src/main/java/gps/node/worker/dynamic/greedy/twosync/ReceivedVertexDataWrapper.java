package gps.node.worker.dynamic.greedy.twosync;

import gps.messages.IncomingBufferedMessage;
import gps.node.worker.dynamic.VertexWrapper;
import gps.writable.MinaWritable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReceivedVertexDataWrapper<V extends MinaWritable> {
	public Map<Integer, VertexWrapper<V>> verticesReceived = new HashMap<Integer, VertexWrapper<V>>();
	public List<IncomingBufferedMessage> exceptionFiles =
		new ArrayList<IncomingBufferedMessage>();
	public List<IncomingBufferedMessage> potentialVerticesToSendMessages =
		new ArrayList<IncomingBufferedMessage>();
}
