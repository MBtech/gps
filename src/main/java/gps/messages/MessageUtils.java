package gps.messages;

import static gps.node.worker.GPSWorkerExposedGlobalVariables.currentSuperstepNo;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.CharacterCodingException;

import gps.node.StatusType;
import gps.node.Utils;

import org.apache.mina.core.buffer.IoBuffer;

public class MessageUtils {

	public static OutgoingBufferedMessage constructLocalMachineIdMessage(int localMachineId) {
		IoBuffer ioBuffer = IoBuffer.allocate(4);
		ioBuffer.putInt(localMachineId);
		return new OutgoingBufferedMessage(MessageTypes.MACHINE_ID_INFORMATION,
			-1 /* dummy superstepNo */, ioBuffer);
	}

	public static OutgoingBufferedMessage constructMessage(MessageTypes type, int superstepNo) {
		OutgoingBufferedMessage outgoingBufferedMessage =
			new OutgoingBufferedMessage(type, superstepNo, null);
		return outgoingBufferedMessage;
	}
	
	public static OutgoingBufferedMessage constructStatusUpdateMessage(int superstepNo,
		StatusType statusType) {
		IoBuffer ioBuffer = IoBuffer.allocate(4);
		ioBuffer.putInt(statusType.getId());
		return new OutgoingBufferedMessage(MessageTypes.STATUS_UPDATE, superstepNo, ioBuffer);
	}

	public static IoBuffer constructMessageHeader(MessageTypes type, int superstepNo,
		int sizeOfMessage) {
		IoBuffer messageHeader = IoBuffer.allocate(12);
		messageHeader.putInt(type.getId());
		messageHeader.putInt(superstepNo);
		messageHeader.putInt(sizeOfMessage);
		return messageHeader;
	}

	public static OutgoingBufferedMessage constructExceptionStatusUpdateMessage(Throwable e)
		throws CharacterCodingException {
		IoBuffer ioBuffer = IoBuffer.allocate(4).setAutoExpand(true);
		ioBuffer.putInt(StatusType.EXCEPTION_THROWN.getId());
		ioBuffer.putString(getStackTrace(e), Utils.ISO_8859_1_ENCODER);
		return new OutgoingBufferedMessage(MessageTypes.STATUS_UPDATE, currentSuperstepNo,
			ioBuffer);
	}	

	private static String getStackTrace(Throwable aThrowable) {
	    Writer result = new StringWriter();
	    PrintWriter printWriter = new PrintWriter(result);
	    aThrowable.printStackTrace(printWriter);
	    return result.toString();
	  }

}
