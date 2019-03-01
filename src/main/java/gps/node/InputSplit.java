package gps.node;

public class InputSplit {

	private final String fileName;
	private final long startOffset;
	private final long endOffset;

	public InputSplit(String fileName, long startOffset, long endOffset) {
		this.fileName = fileName;
		this.startOffset = startOffset;
		this.endOffset = endOffset;
	}

	public String getFileName() {
		return fileName;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public long getEndOffset() {
		return endOffset;
	}
}
