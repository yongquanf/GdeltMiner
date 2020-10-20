package util.Util;

import java.util.Objects;

public   final class TripleGP<T, U,V> {

    public final T first;
    public final U second;
    public final V third;

    public TripleGP(T first, U second,V third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public static <R, S,T> TripleGP<R, S,T> triple(R first, S second, T finalVal) {
        return new TripleGP<>(first, second,finalVal);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        TripleGP<?, ?, ?> tuple = (TripleGP<?, ?, ?>) object;
        return Objects.equals(first, tuple.first) && Objects.equals(second, tuple.second) && Objects.equals(third, tuple.third);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second,third);
    }
}