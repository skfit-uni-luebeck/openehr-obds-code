package de.uksh.medic.etl.settings;

/**
 * Settings for server availability checks.
 *
 * <p>
 * The checks cover the openEHR server and the two FHIR servers (the FHIR Server and the FHIR
 * Terminology Server). Every server that is configured gates the processing of records. The
 * checks are only performed when there is data to process.
 * </p>
 */
public class ServerCheckSettings {
    private static final int DEFAULT_INTERVAL_MS = 5000;
    private static final int DEFAULT_PROBE_TIMEOUT_MS = 5000;
    private static final int DEFAULT_RETRIES = 3;

    private boolean enabled = true;
    private int intervalMs = DEFAULT_INTERVAL_MS;
    private int timeoutMs;
    private int probeTimeoutMs = DEFAULT_PROBE_TIMEOUT_MS;
    private int retries = DEFAULT_RETRIES;

    /**
     * @return true if server availability checks are enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return interval between repeated health checks in milliseconds; a successful check is
     *         also trusted for this duration before the servers are probed again
     */
    public int getIntervalMs() {
        return intervalMs > 0 ? intervalMs : DEFAULT_INTERVAL_MS;
    }

    public void setIntervalMs(int intervalMs) {
        this.intervalMs = intervalMs;
    }

    /**
     * @return interval in milliseconds at which a reminder is logged while processing waits
     *         for a server (0 = only log when processing is paused and when it continues)
     */
    public int getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    /**
     * @return maximum duration of a single health check attempt in milliseconds
     */
    public int getProbeTimeoutMs() {
        return probeTimeoutMs > 0 ? probeTimeoutMs : DEFAULT_PROBE_TIMEOUT_MS;
    }

    public void setProbeTimeoutMs(int probeTimeoutMs) {
        this.probeTimeoutMs = probeTimeoutMs;
    }

    /**
     * @return number of attempts per server before it is declared unavailable
     */
    public int getRetries() {
        return retries > 0 ? retries : DEFAULT_RETRIES;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }
}
