package gps.messages;

import org.apache.mina.core.buffer.IoBuffer;

public class OutgoingBufferedMessage {

	private MessageTypes type;
	private int superstepNo;
	private IoBuffer ioBuffer;

	public OutgoingBufferedMessage(MessageTypes type, int superstepNo, IoBuffer ioBuffer) {
		this.type = type;
		this.ioBuffer = ioBuffer;
		this.superstepNo = superstepNo;
	}

	public MessageTypes getType() {
		return type;
	}

	public IoBuffer getIoBuffer() {
		return ioBuffer;
	}

	public int getSuperstepNo() {
		return superstepNo;
	}
}
