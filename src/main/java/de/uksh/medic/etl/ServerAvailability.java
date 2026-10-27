package de.uksh.medic.etl;

import ca.uhn.fhir.rest.client.api.IGenericClient;
import java.util.concurrent.atomic.AtomicBoolean;
import org.ehrbase.openehr.sdk.client.openehrclient.defaultrestclient.DefaultRestClient;
import org.ehrbase.openehr.sdk.generator.commons.aql.query.Query;
import org.ehrbase.openehr.sdk.response.dto.QueryResponseData;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.UriType;
import org.tinylog.Logger;
import de.uksh.medic.etl.settings.Settings;

/**
 * Monitors availability of openEHR and FHIR Terminology servers.
 * Provides health checks and a background monitoring thread.
 */
public final class ServerAvailability {

    private static final AtomicBoolean OPENEHR_AVAILABLE = new AtomicBoolean(false);
    private static final AtomicBoolean FHIR_TS_AVAILABLE = new AtomicBoolean(false);

    private static DefaultRestClient openEhrClient;
    private static IGenericClient fhirTsClient;
    private static Thread monitorThread;
    private static volatile boolean monitorRunning = true;

    private static final int FAILURES_BEFORE_DOWN = 3;
    private static final int SUCCESSES_BEFORE_UP = 2;

    private static int openEhrFailures = 0;
    private static int openEhrSuccesses = 0;

    private static int fhirFailures = 0;
    private static int fhirSuccesses = 0;

    private ServerAvailability() {
    }

    /**
     * Initialize the server availability monitor.
     *
     * @param newOpenEhrClient the openEHR REST client
     * @param newFhirTsClient  the FHIR terminology client (may be null)
     */
    public static void init(DefaultRestClient newOpenEhrClient, IGenericClient newFhirTsClient) {
        de.uksh.medic.etl.ServerAvailability.openEhrClient = newOpenEhrClient;
        de.uksh.medic.etl.ServerAvailability.fhirTsClient = newFhirTsClient;

        // If openEHR client is not configured, mark it as available
        if (openEhrClient == null) {
            OPENEHR_AVAILABLE.set(true);
        }

        // If FHIR TS client is not configured, mark it as available
        if (fhirTsClient == null) {
            FHIR_TS_AVAILABLE.set(true);
        }
    }

    private static synchronized void updateOpenEhrAvailability(boolean success) {
        boolean wasAvailable = OPENEHR_AVAILABLE.get();

        if (success) {
            openEhrFailures = 0;
            openEhrSuccesses++;

            if (!wasAvailable && openEhrSuccesses >= SUCCESSES_BEFORE_UP) {
                OPENEHR_AVAILABLE.set(true);
                openEhrSuccesses = 0;
                Logger.info("openEHR API is now available");
            }
        } else {
            openEhrSuccesses = 0;
            openEhrFailures++;

            if (wasAvailable && openEhrFailures >= FAILURES_BEFORE_DOWN) {
                OPENEHR_AVAILABLE.set(false);
                openEhrFailures = 0;
                Logger.warn("openEHR API is now unavailable");
            }
        }
    }

    private static synchronized void updateFhirAvailability(boolean success) {
        boolean wasAvailable = FHIR_TS_AVAILABLE.get();

        if (success) {
            fhirFailures = 0;
            fhirSuccesses++;

            if (!wasAvailable && fhirSuccesses >= SUCCESSES_BEFORE_UP) {
                FHIR_TS_AVAILABLE.set(true);
                fhirSuccesses = 0;
                Logger.info("FHIR Terminology Server is now available");
            }
        } else {
            fhirSuccesses = 0;
            fhirFailures++;

            if (wasAvailable && fhirFailures >= FAILURES_BEFORE_DOWN) {
                FHIR_TS_AVAILABLE.set(false);
                fhirFailures = 0;
                Logger.warn("FHIR Terminology Server is now unavailable");
            }
        }
    }

    /**
     * Start a background thread that periodically checks server availability.
     *
     * @param intervalMs interval between checks in milliseconds
     */
    @SuppressWarnings({ "IllegalCatch" })
    public static synchronized void startMonitor(int intervalMs) {
        if (monitorThread != null && monitorThread.isAlive()) {
            Logger.debug("Server availability monitor is already running");
            return;
        }

        monitorThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                if (monitorRunning) {
                    try {
                        // openEHR uses consecutive success/failure thresholds
                        if (openEhrClient != null) {
                            updateOpenEhrAvailability(checkOpenEhr());
                        }

                        // FHIR uses the same consecutive success/failure thresholds
                        if (fhirTsClient != null) {
                            updateFhirAvailability(checkFhirTs());
                        }

                    } catch (Exception e) {
                        Logger.warn(
                                "Error during server availability check: {}",
                                e.getMessage(), e
                        );
                    }
                }

                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "server-availability-monitor");

        monitorThread.setDaemon(true);
        monitorThread.start();

        Logger.info(
                "Server availability monitor started with interval {}ms",
                intervalMs
        );
    }

    /**
     * Pause the background monitor thread.
     * Health checks are suspended but the thread remains alive.
     */
    public static void pauseMonitor() {
        monitorRunning = false;
        Logger.debug("Server availability monitor paused");
    }

    /**
     * Resume the background monitor thread.
     */
    public static void resumeMonitor() {
        monitorRunning = true;
        Logger.debug("Server availability monitor resumed");
    }

    /**
     * Stop the background monitor thread.
     */
    public static void stopMonitor() {
        if (monitorThread != null) {
            monitorThread.interrupt();
            monitorThread = null;
        }
    }

    /**
     * Block until all configured servers are available.
     *
     * @param intervalMs interval between checks in milliseconds
     * @param timeoutMs  maximum time to wait in milliseconds (0 = infinite)
     * @return true if all servers became available, false if timeout reached
     */
    public static boolean waitUntilAvailable(int intervalMs, int timeoutMs) {
        long startTime = System.currentTimeMillis();

        while (true) {
            if (allAvailable()) {
                Logger.info("All required servers are available");
                return true;
            }

            // Log which servers are unavailable
            if (!OPENEHR_AVAILABLE.get() && openEhrClient != null) {
                Logger.info("Waiting for openEHR API to become available...");
            }
            if (!FHIR_TS_AVAILABLE.get() && fhirTsClient != null) {
                Logger.info("Waiting for FHIR Terminology Server to become available...");
            }

            // Check for timeout
            if (timeoutMs > 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= timeoutMs) {
                    Logger.error("Timeout waiting for servers to become available ({}ms)", timeoutMs);
                    return false;
                }
            }

            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * Perform a single availability check for the openEHR API.
     *
     * @return true if the openEHR API is reachable
     */
    @SuppressWarnings({ "IllegalCatch" })
    public static boolean checkOpenEhr() {
        if (openEhrClient == null) {
            return true;
        }
        try {
            QueryResponseData response = openEhrClient.aqlEndpoint().executeRaw(
                    Query.buildNativeQuery("SELECT e/ehr_id/value FROM EHR e LIMIT 1"));
            return response != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Perform a single availability check for the FHIR Terminology Server.
     * Uses the $lookup operation on CodeSystem as a lightweight connectivity test.
     *
     * @return true if the FHIR Terminology Server is reachable
     */
    @SuppressWarnings({ "IllegalCatch" })
    private static boolean checkFhirTs() {
        long startNanos = System.nanoTime();
        String endpoint = String.valueOf(Settings.getFhirServerUrl());

        try {
            if (fhirTsClient == null) {
                Logger.warn("FHIR health check skipped: client is null");
                return false;
            }

            Logger.debug("FHIR health check starting: endpoint={}", endpoint);

            fhirTsClient.forceConformanceCheck();

            long durationMs =
                    (System.nanoTime() - startNanos) / 1_000_000;

            Logger.debug(
                    "FHIR health check succeeded: endpoint={}, durationMs={}",
                    endpoint, durationMs
            );

            return true;

        } catch (Exception e) {
            long durationMs =
                    (System.nanoTime() - startNanos) / 1_000_000;

            Logger.warn(
                    "FHIR health check failed: endpoint={}, durationMs={}, exceptionType={}",
                    endpoint,
                    durationMs,
                    e.getClass().getName(),
                    e
            );

            Throwable cause = e.getCause();
            int depth = 1;

            while (cause != null && depth <= 10) {
                Logger.warn(
                        "FHIR failure cause {}: type={}, message={}",
                        depth,
                        cause.getClass().getName(),
                        cause.getMessage()
                );

                cause = cause.getCause();
                depth++;
            }

            return false;
        }
    }


    /**
     * Return true if all configured servers are currently available.
     */
    public static boolean allAvailable() {
        return OPENEHR_AVAILABLE.get() && FHIR_TS_AVAILABLE.get();
    }

    /**
     * Return true if the openEHR API is currently available.
     */
    public static boolean isOPENEHR_AVAILABLE() {
        return OPENEHR_AVAILABLE.get();
    }

    /**
     * Return true if the FHIR Terminology Server is currently available.
     */
    public static boolean isFHIR_TS_AVAILABLE() {
        return FHIR_TS_AVAILABLE.get();
    }

    /**
     * Mark the openEHR API as unavailable (called when connection errors occur
     * during processing).
     */
    public static synchronized void markOpenEhrUnavailable() {
        OPENEHR_AVAILABLE.set(false);
        openEhrFailures = 0;
        openEhrSuccesses = 0;
    }

    /**
     * Mark the FHIR Terminology Server as unavailable (called when connection
     * errors occur during processing).
     */
    public static synchronized void markFhirTsUnavailable() {
        FHIR_TS_AVAILABLE.set(false);
        fhirFailures = 0;
        fhirSuccesses = 0;
    }

    /**
     * Reset availability flags to false (useful for re-validating after
     * initialization).
     */
    static synchronized void resetAvailability() {
        OPENEHR_AVAILABLE.set(false);
        FHIR_TS_AVAILABLE.set(false);
        openEhrFailures = 0;
        openEhrSuccesses = 0;
        fhirFailures = 0;
        fhirSuccesses = 0;
    }
}