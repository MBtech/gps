package gps.communication.mina;

public class GPSNodeExceptionNotifier {
	
	private Throwable e = null;

	public void setThrowable(Throwable e) {
		this.e = e;
	}
	
	public Throwable getThrowable() {
		return e;
	}
}
