package gps.examples.highestoutdegreecounter;

import java.util.Set;

import org.apache.mina.core.buffer.IoBuffer;

import gps.node.Pair;
import gps.writable.MinaWritable;

// Stores a set of two integer (key, value) pairs, also keeping track of the min value
// in the set.
public class MinTwoIntSetWritable extends MinaWritable {
	private int[] keyArray;
	private int[] valueArray;
	private int minValue = -1;
	private int minValueIndex;

	public MinTwoIntSetWritable() {
		// Every writable that will be used as a message needs to implement a default
		// constructor.
	}
	
	@Override
	public int numBytes() {
		return 0;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int read(byte[] byteArray, int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		// TODO Auto-generated method stub
		
	}

}
