package gps;

import org.easymock.EasyMockSupport;
import org.easymock.IMocksControl;

import junit.framework.TestCase;

public class EasyMockTest extends TestCase {
	protected EasyMockSupport easyMockSupport;
	protected IMocksControl mocksControl;

	@Override
	protected void setUp() {
		TestUtils.init();
		setUp(false /* nice control */);
	}

	protected void setUp(boolean isStrict) {
		easyMockSupport = new EasyMockSupport();
		mocksControl =
			isStrict ? easyMockSupport.createStrictControl() : easyMockSupport.createNiceControl();
		setUpRest();
	}

	// Should be overwritten.
	protected void setUpRest() {
	}

}
