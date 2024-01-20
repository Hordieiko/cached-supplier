package io.github.hordieiko;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.lang.ref.Cleaner;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Represents a supplier of {@link Wrapper <T> auto-closeable wrapper} over the
 * temporary cached instance of the {@link T value} supplied by the {@link #delegate}.
 * <p>
 * The initial instance of the value is created at the first call to {@link #get()}.<br/>
 * The {@link #durationNanos duration} specifies the amount of time for caching.<br/>
 * The {@link #finisher} function will be applied to the cached instance of the {@link T value}
 * once it has expired and all wrappers over it are closed, so it is no longer in use.
 *
 * @param <T> the type of results supplied by this supplier
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class CachedSupplier<T> implements Supplier<CachedSupplier.Wrapper<T>> {
    private final Supplier<T> delegate;
    private final long durationNanos;
    private final Consumer<T> finisher;
    private volatile CachedValue<T> cachedValue;// = CachedValue.empty();
    private final Tracker<T> tracker = new Tracker<>();

    /**
     * Creates an instance of the {@link CachedSupplier}.
     * <p>
     * The {@link #delegate} must supply a new instance of the value when requested
     * since once expired the {@link #finisher} function will be applied to the value
     * which can make it impossible to use anymore.
     *
     * @param delegate the value supplier
     * @param duration the amount of time for caching an instance of the value,
     *                 once expired a new one is created by the next call to the {@link #get()}
     *                 via the {@link #delegate delegate supplier}
     * @param finisher the function that will be applied to the cached instance of the value
     *                 once it has expired and is no longer in use
     * @param <T>      the type of the value
     * @return a new CachedSupplier
     */
    public static <T> CachedSupplier<T> of(@NonNull Supplier<T> delegate,
                                           @NonNull Duration duration,
                                           @NonNull Consumer<T> finisher) {
        if (!duration.isPositive())
            throw new IllegalArgumentException(STR."duration (\{duration.toNanos()} NANOS) must be > 0");
        return new CachedSupplier<>(delegate, duration.toNanos(), finisher);
    }


    /**
     * On the first call, creates a cached instance of the value supplied by the {@link #delegate}.
     * On subsequent calls, it renews the cached value with the new one from the delegate if it has expired.
     * Also applies {@link #finisher} function for all expired values that are no longer in use.
     *
     * @return the auto-closeable wrapper over the temporary cached instance of the value
     */
    @Override
    public Wrapper<T> get() {
        var cv = this.cachedValue;
        if (cv == null) {
            synchronized (this) {
                cv = this.cachedValue;
                if (cv == null) this.cachedValue = cv = CachedValue.of(this.delegate, this.durationNanos);
            }
        } else if (cv.isExpired()) {
            synchronized (this) {
                if (cv.isExpired()) {
                    this.cachedValue = cv = CachedValue.of(this.delegate, this.durationNanos);
                    tracker.asyncCleanupAllExpiredAndClosed(finisher);
                }
            }
        }

        final var spy = new CachedValue.Spy<T>(cv);
        this.tracker.register(spy);
        return new CloseableWrapperImpl(spy);
    }

    /**
     * Represents a cached instance of the {@link T value} supplied by the {@link #delegate}.
     *
     * @param value      cached instance of the {@link T value}
     * @param expiration the value expiration timestamp
     * @param <T>        the type of the value
     */
    private record CachedValue<T>(T value, long expiration) {

        /**
         * Creates a new instance of cached value
         * based on the value from the delegate and cache duration.
         *
         * @param delegate      the value supplier
         * @param durationNanos the amount of time in nanos for caching an instance of the value
         * @param <T>           the type of the value
         * @return a new CachedValue
         */
        static <T> CachedValue<T> of(final Supplier<T> delegate, final long durationNanos) {
            final var value = delegate.get();
            final var expiration = System.nanoTime() + durationNanos;
            return new CachedValue<>(value, expiration);
        }

        /**
         * Verifies if the value has expired at the current time.
         *
         * @return {@code true} if expired, otherwise {@code false}
         */
        boolean isExpired() {return System.nanoTime() - expiration >= 0;}

        /**
         * Represents a spy for a {@link CachedValue cached instance of the value}.
         * <p>
         * Spy is a main part of the {@link Wrapper auto-closeable wrapper}
         * supplied to the client.
         * The {@link #closed} flag reacts to the {@link Wrapper#close() wrapper's close()}.
         *
         * @param <T> the type of the value
         */
        @RequiredArgsConstructor
        static final class Spy<T> {
            final CachedValue<T> cachedValue;
            @Getter
            volatile boolean closed;
        }
    }

    /**
     * Represents an {@link AutoCloseable auto-closeable} temporary cached value wrapper over the
     * temporary cached instance of the {@link T value} supplied by the {@link #delegate}.
     *
     * @param <T> the type of the value
     */
    public sealed interface Wrapper<T> extends AutoCloseable {
        /**
         * Returns the temporary cached instance of the value supplied by the {@link #delegate}.
         *
         * @return the cached instance of the value
         * @apiNote The value mustn't be stored and used outside the usage scope of this wrapper;
         * once the wrapper has been closed or cleaned by the GC, the {@link #finisher} function
         * can be applied to its value which can make it impossible to use anymore.<br/>
         * The value can be safely used while its wrapper is not closed/cleaned by the GC.
         */
        T get();

        /**
         * Applies the {@link #finisher} function to the
         * expired wrapped cached instance of the value.
         */
        @Override
        void close();
    }

    /**
     * The {@link Wrapper temporary cached value wrapper} implementation.
     */
    private final class CloseableWrapperImpl implements Wrapper<T> {
        private static final Cleaner cleaner = Cleaner.create();

        private final CleanableState<T> state;
        private final Cleaner.Cleanable cleanable;

        private CloseableWrapperImpl(CachedValue.Spy<T> cachedValueSpy) {
            this.state = new CleanableState<>(cachedValueSpy, finisher, tracker);
            this.cleanable = cleaner.register(this, state);
        }

        /**
         * Retrieves a wrapped value.
         *
         * @return a wrapped value
         */
        public T get() {return state.cachedValueSpy.cachedValue.value;}

        /**
         * Invokes cleaner action whose main target is to apply the
         * {@link #finisher} function to the expired wrapped cached
         * instance of the value.
         */
        @Override
        public void close() {cleanable.clean();}

        /**
         * Represents a wrapper's state implementing the cleaning action,
         * that mustn't have a reference to the target wrapper that has
         * to be cleaned.
         * <p>
         * The {@link Cleaner cleaner}'s logic is used to close a wrapper
         * on direct calls to {@link #close()} and via GC cleaning.
         * That ensures the {@link #finisher} function is applied to the
         * expired wrapped cached instance of the value even if the wrapper
         * has not been closed properly.
         */
        private record CleanableState<T>(CachedValue.Spy<T> cachedValueSpy,
                                         Consumer<T> finisher,
                                         Tracker<T> tracker) implements Runnable {
            /**
             * Marks the cached value spy closed, then applies the cleanup function,
             * if needed, to the current wrapped value, then asynchronously applies
             * the cleanup function to all others ready to clean.
             */
            @Override
            public void run() {
                final var cvs = this.cachedValueSpy;
                cvs.closed = true;
                cleanupValueIfExpiredAndClosed(tracker, cvs.cachedValue, finisher);
                tracker.asyncCleanupAllExpiredAndClosed(finisher);
            }
        }
    }

    /**
     * The {@code Tracker} controls {@link CachedValue.Spy spies}
     * for a {@link CachedValue cached value} that are supplied inside the {@link Wrapper wrappers}.
     *
     * @param holder a collection to control values supplied inside the {@link Wrapper wrappers}.
     * @param <T>    the type of the value
     */
    private record Tracker<T>(Map<CachedValue<T>, List<CachedValue.Spy<T>>> holder) {
        Tracker() {this(new ConcurrentHashMap<>());}

        /**
         * Registers a spy to the tracker to have control over it.
         *
         * @param spy a wrapper over the {@link CachedValue cached value}
         *            supplied inside the {@link CachedValue closeable wrapper}
         */
        void register(final CachedValue.Spy<T> spy) {
            this.holder.computeIfAbsent(spy.cachedValue, _ -> new ArrayList<>()).add(spy);
        }

        /**
         * Deregister all spies related to the given cached value.
         *
         * @param cachedValue a cached value
         */
        void deregister(final CachedValue<T> cachedValue) {this.holder.remove(cachedValue);}

        /**
         * Verifies if all supplied wrappers related to the given cached value are closed.
         *
         * @param cachedValue a cached value
         * @return {@code true} if all related supplied wrappers are closed, otherwise {@code false}
         */
        boolean isAllSuppliedWrappersClosed(final CachedValue<T> cachedValue) {
            return this.holder.get(cachedValue).stream().allMatch(CachedValue.Spy::isClosed);
        }

        /**
         * Asynchronously applies the cleanup function to all supplied values.
         * <p>
         * The {@link #cleanupValueIfExpiredAndClosed cleanup} function is applied only to those who are ready.
         *
         * @param finisher the function applied to the value when it has expired and is no longer in use
         */
        void asyncCleanupAllExpiredAndClosed(final Consumer<T> finisher) {
            CompletableFuture.runAsync(() -> this.holder.keySet().forEach(cv -> cleanupValueIfExpiredAndClosed(this, cv, finisher)));
        }
    }

    /**
     * If the cached value has expired and all supplied wrappers with its value are closed,
     * then remove the value from the tracker and apply the finisher function to it.
     *
     * @param tracker     controls spies for a cached value that are supplied inside the wrappers
     * @param cachedValue the temporary cached value
     * @param finisher    the function applied to the value when it has expired and is no longer in use
     * @param <T>         the type of the value
     */
    private static <T> void cleanupValueIfExpiredAndClosed(final Tracker<T> tracker,
                                                           final CachedValue<T> cachedValue,
                                                           final Consumer<T> finisher) {
        if (cachedValue.isExpired()) {
            if (tracker.isAllSuppliedWrappersClosed(cachedValue)) {
                tracker.deregister(cachedValue);
                finisher.accept(cachedValue.value);
            }
        }
    }
}
