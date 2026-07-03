package de.uksh.medic.etl.settings;

/**
 * Settings for server availability checks.
 */
public class ServerCheckSettings {
    private static final int DEFAULT_INTERVAL_MS = 5000;

    private boolean enabled = true;
    private int intervalMs = DEFAULT_INTERVAL_MS;
    private int timeoutMs;

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
     * @return interval between health checks in milliseconds
     */
    public int getIntervalMs() {
        return intervalMs > 0 ? intervalMs : DEFAULT_INTERVAL_MS;
    }

    public void setIntervalMs(int intervalMs) {
        this.intervalMs = intervalMs;
    }

    /**
     * @return maximum time to wait for servers at startup in milliseconds (0 =
     *         infinite)
     */
    public int getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
}
