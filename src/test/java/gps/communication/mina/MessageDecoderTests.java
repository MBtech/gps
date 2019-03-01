package gps.communication.mina;

import org.apache.mina.core.buffer.IoBuffer;

import junit.framework.TestCase;

public class MessageDecoderTests extends TestCase {

	@Override
	protected void setUp() {

	}

	public void testPutIoBuffer() {
		IoBuffer dest = IoBuffer.allocate(10);
		dest.putInt(3); // 6 remaining;

	}

}
