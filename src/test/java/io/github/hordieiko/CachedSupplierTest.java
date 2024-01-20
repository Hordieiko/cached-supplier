package io.github.hordieiko;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.ref.Reference;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class CachedSupplierTest {

    private static final Duration TIMEOUT_ONE_SEC = Duration.ofSeconds(1);

    @Test
    void testUnreferencedWrapperCleaned() {
        final var isFinished = new AtomicBoolean();
        final var timeout = TIMEOUT_ONE_SEC;
        final var executorSupplier = CachedSupplier.of(Executors::newSingleThreadExecutor, timeout,
                executor -> {isFinished.set(true); executor.shutdown();});

        var w1 = executorSupplier.get();

        Awaitility.await()
                .timeout(timeout)
                .pollDelay(timeout.minusMillis(1))
                .until(() -> true);

        w1 = null;
        Reference.reachabilityFence(w1);

        Awaitility.await()
                .pollDelay(Duration.ZERO)
                .pollInterval(Duration.ofMillis(10))
                .untilAsserted(() -> {System.gc(); assertTrue(isFinished.get());});
    }

    @Test
    void testCachedValueClosed() {
        final var isFinished = new AtomicBoolean();
        final var timeout = TIMEOUT_ONE_SEC;
        final var executorSupplier = CachedSupplier.of(Executors::newSingleThreadExecutor, timeout,
                executor -> {isFinished.set(true); executor.shutdown();});

        try (var _ = executorSupplier.get()) {
            waiteForAWhile(timeout);
        }

        assertTrue(isFinished.get());
    }

    @Test
    void testSupplierProducesTemporaryCachedValue() {
        final var timeout = TIMEOUT_ONE_SEC;
        final var executorSupplier = CachedSupplier.of(Executors::newSingleThreadExecutor, timeout, ExecutorService::shutdown);

        final var wrapper1 = executorSupplier.get();
        final var wrapper2 = executorSupplier.get();
        final ExecutorService executor1;
        try (wrapper1) {
            executor1 = wrapper1.get();
            final var executor2 = wrapper1.get();
            final var executor3 = wrapper1.get();
            // the value is cached inside the wrapper
            assertEquals(executor2, executor3);
        }

        // different wrappers have one cached value during the cached time
        assertNotEquals(wrapper1, wrapper2);
        final var executor4 = wrapper2.get();
        assertEquals(executor1, executor4);

        waiteForAWhile(timeout);

        // until all wrappers with expired value are not closed/cleaned by the GS the value is safe to use
        final Executable executable = () -> executor1.submit(() -> {});
        assertDoesNotThrow(executable);

        wrapper2.close(); // after a timeout to become expired at the time of the close

        // if a value has expired and all its wrappers have been closed, it is impossible to use it anymore
        assertThrows(RejectedExecutionException.class, executable);

        // expired wrappers still contains their cached values even after they have been closed
        final var executor5 = wrapper2.get();
        assertEquals(executor1, executor5);

        try (final var closeableValue = executorSupplier.get()) {
            final var executor6 = closeableValue.get();
            // once a cached value has expired the supplier renews it with a new one
            assertNotEquals(executor1, executor6);
        }
    }

    private static void waiteForAWhile(final Duration timeout) {
        Awaitility.await()
                .timeout(timeout)
                .pollDelay(timeout.minusMillis(1))
                .until(() -> true);
    }
}