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

    private ServerAvailability() {
    }

    /**
     * Initialize the server availability monitor.
     *
     * @param newOpenEhrClient the openEHR REST client
     * @param newFhirTsClient  the FHIR terminology client (may be null)
     */
    public static void init(DefaultRestClient newOpenEhrClient, IGenericClient newFhirTsClient) {
        ServerAvailability.openEhrClient = newOpenEhrClient;
        ServerAvailability.fhirTsClient = newFhirTsClient;

        // If FHIR TS client is not configured, mark it as available
        if (fhirTsClient == null) {
            FHIR_TS_AVAILABLE.set(true);
        }
    }

    /**
     * Start a background thread that periodically checks server availability.
     *
     * @param intervalMs interval between checks in milliseconds
     */
    @SuppressWarnings({ "IllegalCatch" })
    public static void startMonitor(int intervalMs) {
        monitorThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                // Only perform checks when monitor is running (not paused)
                if (monitorRunning) {
                    try {
                        boolean prevOpenEhr = OPENEHR_AVAILABLE.get();
                        boolean prevFhirTs = FHIR_TS_AVAILABLE.get();

                        if (openEhrClient != null) {
                            OPENEHR_AVAILABLE.set(checkOpenEhr());
                        }
                        if (fhirTsClient != null) {
                            FHIR_TS_AVAILABLE.set(checkFhirTs());
                        }

                        // Log state transitions
                        if (OPENEHR_AVAILABLE.get() && !prevOpenEhr) {
                            Logger.info("openEHR API is now available");
                        } else if (!OPENEHR_AVAILABLE.get() && prevOpenEhr) {
                            Logger.warn("openEHR API is now unavailable");
                        }
                        if (FHIR_TS_AVAILABLE.get() && !prevFhirTs) {
                            Logger.info("FHIR Terminology Server is now available");
                        } else if (!FHIR_TS_AVAILABLE.get() && prevFhirTs) {
                            Logger.warn("FHIR Terminology Server is now unavailable");
                        }
                    } catch (Exception e) {
                        Logger.debug("Error during availability check: {}", e.getMessage());
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
        Logger.info("Server availability monitor started with interval {}ms", intervalMs);
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
    public static boolean checkFhirTs() {
        if (fhirTsClient == null) {
            return true;
        }
        try {
            Parameters params = new Parameters();
            params.addParameter("system", new UriType("http://snomed.info/sct"));
            params.addParameter("code", new CodeType("138875005"));
            fhirTsClient.operation().onType(CodeSystem.class)
                    .named("lookup").withParameters(params).useHttpGet().execute();
            return true;
        } catch (Exception e) {
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
    public static void markOpenEhrUnavailable() {
        OPENEHR_AVAILABLE.set(false);
    }

    /**
     * Mark the FHIR Terminology Server as unavailable (called when connection
     * errors occur during processing).
     */
    public static void markFhirTsUnavailable() {
        FHIR_TS_AVAILABLE.set(false);
    }

    /**
     * Reset availability flags to false (useful for re-validating after
     * initialization).
     */
    static void resetAvailability() {
        OPENEHR_AVAILABLE.set(false);
        FHIR_TS_AVAILABLE.set(false);
    }
}
