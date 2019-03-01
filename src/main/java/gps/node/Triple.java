package gps.node;

public class Triple<A, B, C> extends Pair<A, B> {
	public C trd;

	public Triple(A fst, B snd, C trd) {
		super(fst, snd);
		this.trd = trd;
	}

	@Override
	public String toString() {
		return "Triple[" + fst + "," + snd + "," + trd + "]";
	}

	public boolean equals(Object other) {
		return other instanceof Triple && equals(fst, ((Triple) other).fst)
			&& equals(snd, ((Triple) other).snd) && equals(trd, ((Triple) other).trd);
	}

	public static <A, B, C> Triple<A, B, C> of(A a, B b, C c) {
		return new Triple<A, B, C>(a, b, c);
	}
}
