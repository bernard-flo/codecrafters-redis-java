import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public enum F {
    ;

    public static <T> Consumer<T> wrapEx(ThrowingConsumer<T> throwingConsumer) {
        return (t) -> {
            try {
                throwingConsumer.accept(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T, U> BiConsumer<T, U> wrapEx(ThrowingBiConsumer<T, U> throwingBiConsumer) {
        return (t, u) -> {
            try {
                throwingBiConsumer.accept(t, u);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T, R> Function<T, R> returnNull(ThrowingConsumer<T> throwingConsumer) {
        return (t) -> {
            wrapEx(throwingConsumer).accept(t);
            return null;
        };
    }

    public static <T, U> Consumer<U> bind(ThrowingBiConsumer<T, U> throwingBiConsumer, T t) {
        return (u) -> {
            wrapEx(throwingBiConsumer).accept(t, u);
        };
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }

    @FunctionalInterface
    public interface ThrowingBiConsumer<T, U> {
        void accept(T t, U u) throws Exception;
    }

}
