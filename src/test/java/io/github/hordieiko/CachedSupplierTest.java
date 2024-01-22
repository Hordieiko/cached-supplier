package io.github.hordieiko;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.ref.Reference;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class CachedSupplierTest {

    private static final Duration TIMEOUT_ONE_SEC = Duration.ofSeconds(1L);
    private static final Duration TECHNICAL_TIMEOUT = Duration.ofMillis(200L);

    /**
     * An expired value must be closed even if its wrapper has not been closed properly
     */
    @Test
    void testUnreferencedWrapperCleaned() {
        final var timeout = TIMEOUT_ONE_SEC;
        final var executorSupplier = CachedSupplier.of(Executors::newSingleThreadExecutor, timeout, ExecutorService::shutdown);

        var executorWrapper = executorSupplier.get();
        final var executor = executorWrapper.get();

        Awaitility.await()
                .timeout(timeout)
                .pollDelay(timeout.minusMillis(1))
                .until(() -> true);

        executorWrapper = null;
        Reference.reachabilityFence(executorWrapper);

        Awaitility.await()
                .pollInterval(Duration.ofMillis(10))
                .untilAsserted(() -> {System.gc(); assertTrue(executor::isShutdown);});
    }

    /**
     * A value must be closed once it expires and all related wrappers have been closed
     */
    @Test
    void testCachedValueClosed() {
        final var timeout = TIMEOUT_ONE_SEC;
        final var executorSupplier = CachedSupplier.of(Executors::newSingleThreadExecutor, timeout, ExecutorService::shutdown);

        final ExecutorService executor;
        try (var executorWrapper = executorSupplier.get()) {
            executor = executorWrapper.get();
            waitForAWhile(timeout);
        }

        Awaitility.waitAtMost(TECHNICAL_TIMEOUT)
                .pollInterval(Duration.ofMillis(10))
                .until(executor::isShutdown);
    }

    /**
     * The {@code CachedSupplier} must produce a temporary cached instance of value.
     * The cached value has to be safe to use until all related wrappers are closed and time expires.
     */
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

        waitForAWhile(timeout);

        // until all wrappers with expired value are not closed/cleaned by the GS the value is safe to use
        final Executable executable = () -> executor1.submit(() -> {});
        assertDoesNotThrow(executable);

        wrapper2.close(); // after a timeout to become expired at the time of the close

        // if a value has expired and all its wrappers have been closed, it is impossible to use it anymore
        assertThrows(RejectedExecutionException.class, executable);
        assertTrue(executor1.isShutdown());

        // expired wrappers still contains their cached values even after they have been closed
        final var executor5 = wrapper2.get();
        assertEquals(executor1, executor5);

        try (final var closeableValue = executorSupplier.get()) {
            final var executor6 = closeableValue.get();
            // once a cached value has expired the supplier renews it with a new one
            assertNotEquals(executor1, executor6);
        }
    }

    /**
     * The {@code CachedSupplier.Builder} allows the creation of a {@code CachedSupplier}
     * with a predefined duration and/or finisher function.
     */
    @Test
    void testBuilder() {
        final var timeout = TIMEOUT_ONE_SEC;
        final var executorSupplierWithFinisher = CachedSupplier.builder(Executors::newSingleThreadExecutor)
                .duration(timeout)
                .finisher(ExecutorService::shutdown)
                .build();

        final ExecutorService executor1;
        try (final var executorWrapper = executorSupplierWithFinisher.get()) {
            executor1 = executorWrapper.get();
            waitForAWhile(timeout);
            assertFalse(executor1.isShutdown());
        }

        Awaitility.waitAtMost(TECHNICAL_TIMEOUT)
                .pollInterval(Duration.ofMillis(10))
                .until(executor1::isShutdown); // the finisher function has been applied so the executor is shut down

        final var executorSupplierWithoutFinisher = CachedSupplier.builder(Executors::newSingleThreadExecutor)
                .duration(timeout)
                .build();

        final ExecutorService executor2;
        try (final var executorWrapper = executorSupplierWithoutFinisher.get()) {
            executor2 = executorWrapper.get();
            waitForAWhile(timeout);
            assertFalse(executor2.isShutdown());
        }

        Awaitility.waitAtMost(TECHNICAL_TIMEOUT)
                .pollInterval(Duration.ofMillis(10))
                .untilAsserted(() -> assertFalse(executor2.isShutdown())); // there is no finisher function, so the executor is still active

        // an instance of CachedSupplier with predefined duration and finisher
        final var executorSupplier = CachedSupplier.builder(Executors::newSingleThreadExecutor).build();
        assertNotNull(executorSupplier);
    }

    private static void waitForAWhile(final Duration timeout) {
        Awaitility.await()
                .timeout(timeout)
                .pollDelay(timeout.minusMillis(1))
                .until(() -> true);
    }
}