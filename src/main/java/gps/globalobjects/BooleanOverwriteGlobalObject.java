package gps.globalobjects;

import gps.writable.BooleanWritable;
import gps.writable.MinaWritable;

/**
 *  Overwrite Global Objects use the "consistency model" of CUDA. They start off with default value.
 *  Then on, it will only onlly store the latest value assigned to it. When merging two overwrite
 *  global objects one of the non-null values is picked. When both values are non-null,
 *  one will be picked without any guarantees.
 *  Example: 
 *  bool found = false;
 *  int multiple_of_7 = -1;
 *  for(int i = 0; i < 1000; i++)  { // Parallelized by CUDA
 *    if (A[i] %7 == 0) {
 *      found = true;         // many threads are writing at the same time, the following two lines.
 *      multiple_of_7 = A[i];
 *    }
 *  }
 *  Two cases:
 *  <ul>
 *  	<li> If there are many multiple of 7, in A? say A[0] = 7, A[1] = 14, A[2...999] = 1:
 *  	     In this case, found = true, multipe_of_7 = 7 or 14. The system does not give any
 *  		 guarantees amongst them.
 *      <li> If there are none? say A[0...999] = 1: In that case, found = false,
 *           and multiple_of_7 = -1 as unchanged.
 *  </ul>
 *  TODO(semih): Talk to Sungpack about null values.
 */
public class BooleanOverwriteGlobalObject extends BooleanGlobalObject {

	public BooleanOverwriteGlobalObject() {
		super();
	}
	
	public BooleanOverwriteGlobalObject(boolean value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(((BooleanWritable) otherValue));	
	}
}
