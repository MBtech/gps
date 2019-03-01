package gps.communication.mina;

import gps.messages.MessageTypes;
import gps.messages.MessageUtils;
import gps.messages.OutgoingBufferedMessage;
import gps.node.MachineStats.StatName;
import gps.node.worker.GPSWorkerExposedGlobalVariables;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

public class MessageEncoder implements ProtocolEncoder {

	@Override
	public void encode(IoSession session, Object message,
		ProtocolEncoderOutput protocolEncoderOutput) throws Exception {
		OutgoingBufferedMessage outgoingBufferedMessage = (OutgoingBufferedMessage) message;
		MessageTypes type = outgoingBufferedMessage.getType();
		if (MessageTypes.DATA != type && MessageTypes.VERTEX_SHUFFLING_WITH_DATA != type) {
			protocolEncoderOutput.flush();
		}
		IoBuffer ioBuffer = outgoingBufferedMessage.getIoBuffer();
		if (ioBuffer != null) {
			ioBuffer.flip();
		}
		int sizeOfMessage = ioBuffer == null ? 0 : ioBuffer.limit() - ioBuffer.position();
		IoBuffer messageHeader = MessageUtils.constructMessageHeader(type,
			outgoingBufferedMessage.getSuperstepNo(), sizeOfMessage);
		messageHeader.flip();
		protocolEncoderOutput.write(messageHeader);
		if (ioBuffer != null && sizeOfMessage > 0) {
			protocolEncoderOutput.write(ioBuffer);
		}
		// TODO(semih): put back && MessageTypes.EXCEPTIONS_MAP != type
		if (MessageTypes.DATA != type && MessageTypes.VERTEX_SHUFFLING_WITH_DATA != type
			&& MessageTypes.INITIAL_VERTEX_PARTITIONING != type) {
			protocolEncoderOutput.flush();
		} else {
			GPSWorkerExposedGlobalVariables.getMachineStats().updateStatForSuperstep(
				StatName.TOTAL_BYTES_SENT, outgoingBufferedMessage.getSuperstepNo(),
				(double) sizeOfMessage); // (double) sizeOfMessage + messageHeader.limit()
			if (MessageTypes.DATA == type) {
				GPSWorkerExposedGlobalVariables.getMachineStats().updateStatForSuperstep(
					StatName.TOTAL_DATA_BYTES_SENT, outgoingBufferedMessage.getSuperstepNo(),
					(double) sizeOfMessage);
			}
		}
	}

	@Override
	public void dispose(IoSession session) throws Exception {
		// nothing to dispose
	}
}
