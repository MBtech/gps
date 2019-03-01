package gps.node.worker.dynamic.greedy.onesync;

import gps.messages.IncomingBufferedMessage;
import gps.node.worker.dynamic.VertexWrapper;
import gps.writable.MinaWritable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReceivedVertexDataWrapper<V extends MinaWritable> {
//	public Map<Integer, VertexWrapper> verticesReceivedInCurrentSuperstep =
//		new HashMap<Integer, VertexWrapper>();
	public Map<Integer, VertexWrapper<V>> verticesReceived = new HashMap<Integer, VertexWrapper<V>>();
	public List<IncomingBufferedMessage> exceptionFilesAndPotentialVerticesToSendMessages =
		new ArrayList<IncomingBufferedMessage>();
}
