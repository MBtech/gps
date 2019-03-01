package gps.communication.mina;

import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.node.MachineStats.StatName;
import gps.node.worker.GPSWorkerExposedGlobalVariables;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

public class MessageDecoder extends CumulativeProtocolDecoder {

	private static Logger logger = Logger.getLogger(MessageDecoder.class);

	private static final String DECODER_STATE_KEY = MessageDecoder.class.getName() + ".STATE";

	private static class DecoderState {
		MessageTypes type = null;
		Integer superstepNo = null;
		Integer messageLength = null;
		long decodingStartTime = -1;

		public void clear() {
			this.type = null;
			this.superstepNo = null;
			this.messageLength = null;
		}
	}

	@Override
	protected boolean doDecode(IoSession session, IoBuffer ioBuffer,
		ProtocolDecoderOutput protocolDecoderOutput) throws Exception {
		DecoderState decoderState = (DecoderState) session.getAttribute(DECODER_STATE_KEY);
		if (decoderState == null) {
			decoderState = new DecoderState();
			session.setAttribute(DECODER_STATE_KEY, decoderState);
		}
		int available = ioBuffer.remaining();
		Integer fromMachineId = (Integer) session.getAttribute("machineId");
		MessageTypes type = decoderState.type;
		if (type == null) {
//			if (fromMachineId != null && fromMachineId == ((GPSWorkerExposedGlobalVariables.getLocalMachineId() + 1) % GPSWorkerExposedGlobalVariables.getNumWorkers())) {
//				decoderState.decodingStartTime = System.currentTimeMillis();					
//			}
			if (available >= 4) {
				type = MessageTypes.getTypeFromId(ioBuffer.getInt());
				decoderState.type = type;
				available -= 4;
			} else {
				return false;
			}
		}

		logger.debug("Decoding a message from machineId: " + fromMachineId + " of type: " + type);

		if (decoderState.superstepNo == null) {
			if (available >= 4) {
				decoderState.superstepNo = ioBuffer.getInt();
				if (fromMachineId != null &&
					fromMachineId == ((GPSWorkerExposedGlobalVariables.getLocalMachineId() + 1) % GPSWorkerExposedGlobalVariables.getNumWorkers())
					&& (MessageTypes.DATA == type) && decoderState.decodingStartTime == -1) {
					decoderState.decodingStartTime = System.currentTimeMillis();
				}
				available -= 4;
			} else {
				logger.error("4 bytes of the superstep header is not available!!!");
				return false;
			}
		}

		if (decoderState.messageLength == null) {
			if (available >= 4) {
				int messageLength = ioBuffer.getInt();
				decoderState.messageLength = messageLength;
				available -= 4;
			} else {
				logger.error("4 bytes of the data message size header is not available!!!");
				return false;
			}
		}

		if (available >= decoderState.messageLength) {
			IoBuffer slice = ioBuffer.getSlice(ioBuffer.position(), decoderState.messageLength);
			protocolDecoderOutput.write(new IncomingBufferedMessage(type, decoderState.superstepNo,
				slice));
			ioBuffer.skip(decoderState.messageLength);
			if (MessageTypes.DATA == decoderState.type
				|| MessageTypes.EXCEPTIONS_MAP == decoderState.type ||
				MessageTypes.VERTEX_SHUFFLING_WITH_DATA == decoderState.type ||
				MessageTypes.LARGE_VERTEX_DATA == decoderState.type ||
				MessageTypes.FINAL_DATA_SENT == decoderState.type) {
				GPSWorkerExposedGlobalVariables.getMachineStats().updateStatForSuperstep(
					StatName.TOTAL_BYTES_RECEIVED, decoderState.superstepNo == null ? -1 : decoderState.superstepNo,
					(double) decoderState.messageLength);
				if (MessageTypes.FINAL_DATA_SENT == decoderState.type && fromMachineId == ((GPSWorkerExposedGlobalVariables.getLocalMachineId() + 1) % GPSWorkerExposedGlobalVariables.getNumWorkers())) {
					GPSWorkerExposedGlobalVariables.getMachineStats().updateStatForSuperstep(
						StatName.TOTAL_DECODER_STATE_TIME, decoderState.superstepNo == null ? -1 : decoderState.superstepNo,
						(double) (System.currentTimeMillis() - decoderState.decodingStartTime));
					decoderState.decodingStartTime = -1;
				}
			}
			if (MessageTypes.DATA == decoderState.type) {
				GPSWorkerExposedGlobalVariables.getMachineStats().updateStatForSuperstep(
					StatName.TOTAL_DATA_BYTES_RECEIVED, decoderState.superstepNo,
					(double) decoderState.messageLength);
			}
			decoderState.clear();
			return true;
		} else {
			logger.debug("the entire message has not arrived yet. available: " + available
				+ " expected: " + decoderState.messageLength);
			return false;
		}
	}
}
