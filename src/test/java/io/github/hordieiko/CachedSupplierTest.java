package io.github.hordieiko;

import lombok.Getter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.ref.Reference;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class CachedSupplierTest {

    private static final Duration TIMEOUT = Duration.ofMillis(100L);

    /**
     * An expired value must be closed even if its wrapper has not been closed properly
     */
    @Test
    void testUnreferencedWrapperCleaned() {
        final var cachedSupplier = CachedSupplier.of(CloseableService::new, TIMEOUT, CloseableService::close);

        var wrapper = cachedSupplier.get();
        final var value = wrapper.get();

        waitForAWhile();

        wrapper = null;
        Reference.reachabilityFence(wrapper);

        Awaitility.await()
                .pollInterval(Duration.ofMillis(10))
                .untilAsserted(() -> {System.gc(); assertTrue(value.isClosed());});
    }

    /**
     * A value must be closed once it expires and all related wrappers have been closed
     */
    @Test
    void testCachedValueClosed() {
        final var cachedSupplier = CachedSupplier.of(CloseableService::new, TIMEOUT, CloseableService::close);

        final CloseableService value;
        try (var wrapper = cachedSupplier.get()) {
            value = wrapper.get();
            waitForAWhile();
        }

        Awaitility.waitAtMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(10))
                .until(value::isClosed);
    }

    /**
     * The {@code CachedSupplier} must produce a temporary cached instance of value.
     * The cached value has to be safe to use until all related wrappers are closed and time expires.
     */
    @Test
    void testSupplierProducesTemporaryCachedValue() {
        final var cachedSupplier = CachedSupplier.of(CloseableService::new, TIMEOUT, CloseableService::close);

        final var wrapper1 = cachedSupplier.get();
        final var wrapper2 = cachedSupplier.get();
        final CloseableService value1;
        try (wrapper1) {
            value1 = wrapper1.get();
            final var value2 = wrapper1.get();
            final var value3 = wrapper1.get();
            // the value is cached inside the wrapper
            assertSame(value2, value3);
        }

        // different wrappers have one cached value during the cached time
        assertNotSame(wrapper1, wrapper2);
        final var value4 = wrapper2.get();
        assertSame(value1, value4);

        waitForAWhile();

        // until all wrappers with expired value are not closed/cleaned by the GS the value is safe to use
        final Executable executable = value1::execute;
        assertDoesNotThrow(executable);

        wrapper2.close(); // after a timeout to become expired at the time of the close

        // if a value has expired and all its wrappers have been closed, it is impossible to use it anymore
        assertThrows(IllegalStateException.class, executable);
        assertTrue(value1.isClosed());

        // expired wrappers still contains their cached values even after they have been closed
        final var value5 = wrapper2.get();
        assertSame(value1, value5);

        try (final var wrapper = cachedSupplier.get()) {
            final var value6 = wrapper.get();
            // once a cached value has expired the supplier renews it with a new one
            assertNotSame(value1, value6);
        }
    }

    /**
     * The {@code CachedSupplier.Builder} allows the creation of a {@code CachedSupplier}
     * with a predefined duration and/or finisher function.
     */
    @Test
    void testBuilder() {
        final var supplierWithFinisher =
                CachedSupplier.builder(CloseableService::new)
                        .duration(TIMEOUT)
                        .finisher(CloseableService::close)
                        .build();

        final CloseableService value1;
        try (final var wrapper = supplierWithFinisher.get()) {
            value1 = wrapper.get();
            waitForAWhile();
            assertFalse(value1.isClosed());
        }

        Awaitility.waitAtMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(10))
                .until(value1::isClosed); // the finisher function has been applied so the service is closed

        final var supplierWithoutFinisher =
                CachedSupplier.builder(CloseableService::new)
                        .duration(TIMEOUT)
                        .build();

        final CloseableService value2;
        try (final var wrapper = supplierWithoutFinisher.get()) {
            value2 = wrapper.get();
            waitForAWhile();
            assertFalse(value2.isClosed());
        }

        Awaitility.waitAtMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(10))
                .untilAsserted(() -> assertFalse(value2.isClosed())); // there is no finisher function, so the service is still active

        // an instance of CachedSupplier with predefined duration and finisher
        final var supplier = CachedSupplier.builder(CloseableService::new).build();
        assertNotNull(supplier);
    }

    private static void waitForAWhile() {
        Awaitility.await()
                .timeout(TIMEOUT)
                .pollDelay(TIMEOUT.minusMillis(1))
                .until(() -> true);
    }

    @Getter
    private static final class CloseableService implements AutoCloseable {
        volatile boolean closed;

        void execute() throws IllegalStateException {if (closed) throw new IllegalStateException();}

        @Override
        public void close() {closed = true;}
    }
}