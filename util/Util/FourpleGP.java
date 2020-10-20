package util.Util;

import java.util.Objects;

public   final class FourpleGP<T, U,V,W> {

    public final T first;
    public final U second;
    public final V third;
    public final W fourth;

    private FourpleGP(T first, U second,V third, W fourth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
    }

    public static <R, S,T,P> FourpleGP<R, S,T,P> fourple(R first, S second, T finalVal,P againValue) {
        return new FourpleGP<>(first, second,finalVal, againValue);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        FourpleGP<?, ?, ?, ?> tuple = (FourpleGP<?, ?, ?, ?>) object;
        return Objects.equals(first, tuple.first) && Objects.equals(second, tuple.second) && Objects.equals(third, tuple.third)&& Objects.equals(fourth, tuple.fourth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second,third,fourth);
    }

	public String asRecord() {
		// TODO Auto-generated method stub
		return first+","+second+","+third+","+fourth;
	}
}