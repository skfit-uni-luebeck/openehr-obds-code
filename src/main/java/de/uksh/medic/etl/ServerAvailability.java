package de.uksh.medic.etl;

import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IEntityResult;
import de.uksh.medic.etl.settings.Settings;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.ehrbase.openehr.sdk.client.openehrclient.defaultrestclient.DefaultRestClient;
import org.ehrbase.openehr.sdk.generator.commons.aql.query.Query;
import org.tinylog.Logger;

/**
 * Monitors the three servers this toolkit depends on: the openEHR server and two FHIR
 * servers — the FHIR Server (data, {@code fhirServerUrl}) and the FHIR Terminology Server
 * ({@code fhirTsUrl}).
 *
 * <p>
 * Checks are performed on demand, not on a background timer: they only run when there is
 * actual data to process. A positive verdict is trusted for {@code intervalMs}, a negative
 * verdict is always re-checked against the server, so a stale flag can never block
 * processing of an already reachable server.
 * </p>
 *
 * <p>
 * A server counts as available when it answers with an HTTP 2xx response. Every message
 * names the server role and its URL, so the FHIR Server and the FHIR Terminology Server can
 * never be confused — even if both settings point to the same host.
 * </p>
 */
public final class ServerAvailability {

    /**
     * The servers monitored by this class. There are exactly three: one openEHR server and
     * two FHIR servers.
     */
    public enum ServerKind {

        /** The openEHR repository the compositions are written to. */
        OPENEHR_SERVER("openEHR server", "openEhrUrl", "openEhrServer"),
        /** The FHIR data server used by the mapping scripts, e.g. for Medication lookups. */
        FHIR_SERVER("FHIR Server", "fhirServerUrl", "fhirServer"),
        /** The FHIR terminology server used for CodeSystem and ConceptMap operations. */
        FHIR_TERMINOLOGY_SERVER("FHIR Terminology Server", "fhirTsUrl", "fhirTerminologyServer");

        private final String label;
        private final String settingsKey;
        private final String healthKey;

        ServerKind(String newLabel, String newSettingsKey, String newHealthKey) {
            this.label = newLabel;
            this.settingsKey = newSettingsKey;
            this.healthKey = newHealthKey;
        }

        /**
         * @return the human readable name used in every log message
         */
        public String label() {
            return label;
        }

        /**
         * @return the settings key the URL of this server is configured with
         */
        public String settingsKey() {
            return settingsKey;
        }

        /**
         * @return the key of this server in the {@code /health} response
         */
        public String healthKey() {
            return healthKey;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    /**
     * A single connectivity probe for one server.
     */
    @FunctionalInterface
    interface Probe {

        /**
         * Perform the probe.
         *
         * @return null if the server answered with HTTP 2xx, otherwise a short failure reason
         */
        String check();
    }

    private static final int HTTP_OK_MIN = 200;
    private static final int HTTP_OK_MAX = 300;
    private static final int DEFAULT_PROBE_TIMEOUT_MS = 5000;
    private static final int DEFAULT_RETRIES = 3;
    private static final int ATTEMPT_BACKOFF_MS = 250;
    private static final int MS_PER_SECOND = 1000;
    private static final int LABEL_COLUMN_WIDTH = 26;
    private static final String METADATA_PATH = "metadata";
    private static final String AQL_PING = "SELECT e/ehr_id/value FROM EHR e LIMIT 1";
    private static final String STATE_OK = "ok";
    private static final String STATE_DOWN = "down";
    private static final String STATE_UNKNOWN = "unknown";
    private static final String STATE_NOT_CONFIGURED = "not-configured";
    private static final int MIN_PRINTABLE_CHAR = 0x20;

    private static final ExecutorService PROBE_EXECUTOR = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "server-availability-probe");
        thread.setDaemon(true);
        return thread;
    });

    /**
     * A monitored server together with its last verdict.
     */
    private static final class MonitoredServer {

        private final ServerKind kind;
        private final String url;
        private final Probe probe;
        private final boolean configured;
        private final AtomicBoolean available = new AtomicBoolean(true);
        private volatile String lastError;
        private volatile long lastCheckEpochMs;

        private MonitoredServer(ServerKind newKind, String newUrl, Probe newProbe) {
            this.kind = newKind;
            this.url = newUrl;
            this.probe = newProbe;
            this.configured = newProbe != null;
        }
    }

    private static volatile Map<ServerKind, MonitoredServer> servers = emptyRegistry();
    private static volatile int probeTimeoutMs = DEFAULT_PROBE_TIMEOUT_MS;
    private static volatile int retries = DEFAULT_RETRIES;
    private static final AtomicBoolean NO_TERMINOLOGY_SERVER_LOGGED = new AtomicBoolean(false);

    private ServerAvailability() {
    }

    /**
     * Configure how the probes are executed.
     *
     * @param newProbeTimeoutMs maximum duration of a single probe attempt in milliseconds
     * @param newRetries number of attempts before a server is declared unavailable
     */
    public static void configure(int newProbeTimeoutMs, int newRetries) {
        probeTimeoutMs = newProbeTimeoutMs > 0 ? newProbeTimeoutMs : DEFAULT_PROBE_TIMEOUT_MS;
        retries = newRetries > 0 ? newRetries : DEFAULT_RETRIES;
    }

    /**
     * Register the servers to monitor. A server whose client is null is not configured and
     * therefore never blocks processing.
     *
     * @param openEhrClient the openEHR REST client
     * @param fhirClient the FHIR data server client (may be null)
     * @param fhirTsClient the FHIR terminology server client (may be null)
     */
    public static void init(DefaultRestClient openEhrClient, IGenericClient fhirClient, IGenericClient fhirTsClient) {
        Map<ServerKind, MonitoredServer> registry = emptyRegistry();
        String openEhrUrl = Settings.getOpenEhrUrl() == null ? null : Settings.getOpenEhrUrl().toString();
        String fhirServerUrl = Settings.getFhirServerUrl() == null ? null : Settings.getFhirServerUrl().toString();
        String fhirTsUrl = Settings.getFhirTsUrl() == null ? null : Settings.getFhirTsUrl().toString();

        registry.put(ServerKind.OPENEHR_SERVER, new MonitoredServer(ServerKind.OPENEHR_SERVER, openEhrUrl,
                openEhrClient == null ? null : () -> probeOpenEhr(openEhrClient)));
        registry.put(ServerKind.FHIR_SERVER, new MonitoredServer(ServerKind.FHIR_SERVER, fhirServerUrl,
                fhirClient == null ? null : () -> probeFhir(fhirClient)));
        registry.put(ServerKind.FHIR_TERMINOLOGY_SERVER, new MonitoredServer(ServerKind.FHIR_TERMINOLOGY_SERVER,
                fhirTsUrl, fhirTsClient == null ? null : () -> probeFhir(fhirTsClient)));

        servers = registry;
    }

    /**
     * Log which servers are monitored, one line per server, so that the openEHR server and
     * the two FHIR servers are visible separately.
     */
    public static void logConfiguration() {
        int openEhrServers = 0;
        int fhirServers = 0;
        for (MonitoredServer server : servers.values()) {
            if (!server.configured) {
                continue;
            }
            if (server.kind == ServerKind.OPENEHR_SERVER) {
                openEhrServers++;
            } else {
                fhirServers++;
            }
        }
        Logger.info("Availability checks: {} openEHR server + {} FHIR servers, checked when data arrives",
                openEhrServers, fhirServers);
        for (MonitoredServer server : servers.values()) {
            if (server.configured) {
                Logger.info("  {} {}", pad(server.kind.label()), server.url);
            } else {
                Logger.info("  Not monitored (not configured): {} (settings key {})", server.kind.label(),
                        server.kind.settingsKey());
            }
        }
    }

    /**
     * Stop processing until every configured server answers with HTTP 2xx again. This method
     * never gives up and never reports an error: it logs one message when processing is
     * paused and one message when it continues.
     *
     * <p>
     * Positive verdicts that are younger than {@code intervalMs} are trusted, so a healthy
     * setup is probed at most once per {@code intervalMs}.
     * </p>
     *
     * @param intervalMs pause between repeated checks and validity of a positive verdict
     * @param warnAfterMs log a reminder at this interval while waiting; 0 logs only the pause
     *                    and the resume message
     */
    public static void awaitServers(int intervalMs, int warnAfterMs) {
        long startTime = System.currentTimeMillis();
        boolean paused = false;
        long nextWarning = warnAfterMs > 0 ? startTime + warnAfterMs : 0;

        while (true) {
            boolean allAvailable = true;
            for (MonitoredServer server : servers.values()) {
                if (!server.configured) {
                    continue;
                }
                if (!isFreshPositive(server, intervalMs)) {
                    runCheck(server);
                }
                if (!server.available.get()) {
                    allAvailable = false;
                }
            }

            if (allAvailable) {
                if (paused) {
                    Logger.info("Resuming processing after {}s: all servers are available again: {}",
                            (System.currentTimeMillis() - startTime) / MS_PER_SECOND, describeAvailable());
                }
                return;
            }

            if (!paused) {
                paused = true;
                Logger.warn("Pausing processing until all servers are available. Unavailable: {}. Available: {}",
                        describeUnavailable(), describeAvailable());
            }

            if (nextWarning > 0 && System.currentTimeMillis() >= nextWarning) {
                Logger.warn("Still waiting for servers after {}ms. Unavailable: {}",
                        System.currentTimeMillis() - startTime, describeUnavailable());
                nextWarning += warnAfterMs;
            }

            sleep(intervalMs);
        }
    }

    /**
     * Return true if all configured servers are currently considered available. This reads
     * the cached verdicts and does not probe.
     *
     * @return true if no configured server is marked unavailable
     */
    public static boolean allAvailable() {
        for (MonitoredServer server : servers.values()) {
            if (server.configured && !server.available.get()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Probe all configured servers immediately, ignoring cached verdicts.
     *
     * @return true if every configured server answered with HTTP 2xx
     */
    public static boolean checkAll() {
        for (MonitoredServer server : servers.values()) {
            if (server.configured) {
                runCheck(server);
            }
        }
        return allAvailable();
    }

    /**
     * Return true if the openEHR server is currently considered available.
     *
     * @return true if the openEHR server is available
     */
    public static boolean isOpenEhrAvailable() {
        return isAvailable(ServerKind.OPENEHR_SERVER);
    }

    /**
     * Return true if the FHIR data server is currently considered available.
     *
     * @return true if the FHIR Server is available
     */
    public static boolean isFhirServerAvailable() {
        return isAvailable(ServerKind.FHIR_SERVER);
    }

    /**
     * Return true if the FHIR terminology server is currently considered available.
     *
     * @return true if the FHIR Terminology Server is available
     */
    public static boolean isFhirTsAvailable() {
        return isAvailable(ServerKind.FHIR_TERMINOLOGY_SERVER);
    }

    /**
     * Return true if the given server is available or not configured.
     *
     * @param kind the server to inspect
     * @return true if the server is available or not configured
     */
    public static boolean isAvailable(ServerKind kind) {
        MonitoredServer server = servers.get(kind);
        return server == null || server.available.get();
    }

    /**
     * Mark the openEHR server as unavailable after an error during processing.
     *
     * @param reason short description of the failure
     */
    public static void markOpenEhrUnavailable(String reason) {
        markUnavailable(ServerKind.OPENEHR_SERVER, reason);
    }

    /**
     * Mark the FHIR data server as unavailable after an error during processing.
     *
     * @param reason short description of the failure
     */
    public static void markFhirServerUnavailable(String reason) {
        markUnavailable(ServerKind.FHIR_SERVER, reason);
    }

    /**
     * Mark the FHIR terminology server as unavailable after an error during processing.
     *
     * @param reason short description of the failure
     */
    public static void markFhirTsUnavailable(String reason) {
        markUnavailable(ServerKind.FHIR_TERMINOLOGY_SERVER, reason);
    }

    /**
     * Describe all configured servers that are currently unavailable, including URL and the
     * reason of the last failure.
     *
     * @return comma separated list of unavailable servers or "none"
     */
    public static String describeUnavailable() {
        return describe(false);
    }

    /**
     * Describe all configured servers that are currently available, including their URLs.
     *
     * @return comma separated list of available servers or "none"
     */
    public static String describeAvailable() {
        return describe(true);
    }

    /**
     * Build the JSON object of the {@code /health} response describing every server.
     *
     * @return a JSON object with one entry per server
     */
    public static String statusJson() {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (MonitoredServer server : servers.values()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append('"').append(server.kind.healthKey()).append("\":{")
                    .append("\"url\":").append(jsonString(server.url)).append(',')
                    .append("\"configured\":").append(server.configured).append(',')
                    .append("\"available\":").append(server.available.get()).append(',')
                    .append("\"state\":").append(jsonString(stateOf(server))).append(',')
                    .append("\"lastCheckEpochMs\":").append(server.lastCheckEpochMs).append(',')
                    .append("\"lastError\":").append(jsonString(server.lastError))
                    .append('}');
        }
        return json.append('}').toString();
    }

    /**
     * Register a probe for a single server. Used by {@link #init} and by tests.
     *
     * @param kind the server to register
     * @param url the URL to report for this server
     * @param probe the probe to run, or null if the server is not configured
     */
    static synchronized void register(ServerKind kind, String url, Probe probe) {
        servers = replaceEntry(kind, url, probe);
    }

    /**
     * Drop all verdicts and forget every configured server.
     */
    static synchronized void reset() {
        servers = emptyRegistry();
        NO_TERMINOLOGY_SERVER_LOGGED.set(false);
    }

    /**
     * Report whether an HTTP status code counts as available.
     *
     * @param statusCode the HTTP status code of the probe response
     * @return true for 2xx status codes
     */
    static boolean isHttpSuccess(int statusCode) {
        return statusCode >= HTTP_OK_MIN && statusCode < HTTP_OK_MAX;
    }

    /**
     * Log once that no FHIR terminology server is configured.
     */
    public static void logMissingTerminologyServer() {
        if (NO_TERMINOLOGY_SERVER_LOGGED.compareAndSet(false, true)) {
            Logger.warn("No {} configured ({} is empty), code systems and concept maps cannot be checked",
                    ServerKind.FHIR_TERMINOLOGY_SERVER.label(), ServerKind.FHIR_TERMINOLOGY_SERVER.settingsKey());
        }
    }

    private static Map<ServerKind, MonitoredServer> emptyRegistry() {
        Map<ServerKind, MonitoredServer> registry = new EnumMap<>(ServerKind.class);
        for (ServerKind kind : ServerKind.values()) {
            registry.put(kind, new MonitoredServer(kind, null, null));
        }
        return registry;
    }

    private static Map<ServerKind, MonitoredServer> replaceEntry(ServerKind kind, String url, Probe probe) {
        Map<ServerKind, MonitoredServer> registry = new EnumMap<>(ServerKind.class);
        registry.putAll(servers);
        registry.put(kind, new MonitoredServer(kind, url, probe));
        return registry;
    }

    private static boolean isFreshPositive(MonitoredServer server, int intervalMs) {
        return server.available.get() && server.lastCheckEpochMs > 0
                && System.currentTimeMillis() - server.lastCheckEpochMs < intervalMs;
    }

    /**
     * Run a single check for one server, including retries and the watchdog timeout.
     *
     * @param server the server to check
     */
    private static void runCheck(MonitoredServer server) {
        // While a server is already known to be down the repeated failures are only logged
        // in debug level, so an outage does not flood the log with error messages.
        boolean isNewOutage = server.available.get();
        String failure = null;
        for (int attempt = 1; attempt <= retries; attempt++) {
            failure = runProbe(server);
            if (failure == null) {
                setAvailable(server, true, null);
                return;
            }
            if (attempt < retries) {
                logAttemptFailure(isNewOutage, server, attempt, failure);
                sleep(ATTEMPT_BACKOFF_MS * attempt);
            }
        }
        if (isNewOutage) {
            Logger.warn("{} [{}] probe failed after {} attempts: {}", server.kind.label(), server.url, retries,
                    failure);
        } else {
            Logger.debug("{} [{}] probe still failing after {} attempts: {}", server.kind.label(), server.url,
                    retries, failure);
        }
        setAvailable(server, false, failure);
    }

    private static void logAttemptFailure(boolean isNewOutage, MonitoredServer server, int attempt, String failure) {
        if (isNewOutage) {
            Logger.warn("{} [{}] probe attempt {}/{} failed: {}", server.kind.label(), server.url, attempt, retries,
                    failure);
        } else {
            Logger.debug("{} [{}] probe attempt {}/{} still failing: {}", server.kind.label(), server.url, attempt,
                    retries, failure);
        }
    }

    private static String runProbe(MonitoredServer server) {
        Future<String> future = PROBE_EXECUTOR.submit(server.probe::check);
        try {
            return future.get(probeTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            return "timed out after " + probeTimeoutMs + "ms";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "interrupted";
        } catch (ExecutionException e) {
            return describeThrowable(e.getCause() == null ? e : e.getCause());
        }
    }

    private static void setAvailable(MonitoredServer server, boolean available, String error) {
        server.lastError = error;
        server.lastCheckEpochMs = System.currentTimeMillis();
        boolean changed = server.available.getAndSet(available) != available;
        if (!changed) {
            return;
        }
        if (available) {
            Logger.info("{} [{}] is now available", server.kind.label(), server.url);
        } else {
            Logger.warn("{} [{}] is now unavailable ({})", server.kind.label(), server.url, error);
        }
    }

    private static void markUnavailable(ServerKind kind, String reason) {
        MonitoredServer server = servers.get(kind);
        if (server == null || !server.configured) {
            return;
        }
        setAvailable(server, false, reason);
    }

    private static String describe(boolean available) {
        List<String> parts = new ArrayList<>();
        for (MonitoredServer server : servers.values()) {
            if (!server.configured || server.available.get() != available) {
                continue;
            }
            StringBuilder part = new StringBuilder(server.kind.label()).append(" [").append(server.url).append(']');
            if (!available && server.lastError != null) {
                part.append(" (").append(server.lastError).append(')');
            }
            parts.add(part.toString());
        }
        return parts.isEmpty() ? "none" : String.join(", ", parts);
    }

    private static String stateOf(MonitoredServer server) {
        if (!server.configured) {
            return STATE_NOT_CONFIGURED;
        }
        if (server.lastCheckEpochMs == 0) {
            return STATE_UNKNOWN;
        }
        return server.available.get() ? STATE_OK : STATE_DOWN;
    }

    @SuppressWarnings({"IllegalCatch"})
    private static String probeOpenEhr(DefaultRestClient openEhrClient) {
        try {
            openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(AQL_PING));
            return null;
        } catch (Exception e) {
            return describeThrowable(e);
        }
    }

    @SuppressWarnings({"IllegalCatch"})
    private static String probeFhir(IGenericClient client) {
        try {
            IEntityResult result = client.rawHttpRequest().get(METADATA_PATH).execute();
            if (isHttpSuccess(result.getStatusCode())) {
                return null;
            }
            return "HTTP " + result.getStatusCode();
        } catch (Exception e) {
            return describeThrowable(e);
        }
    }

    private static String describeThrowable(Throwable throwable) {
        String message = throwable.getMessage();
        return message == null || message.isEmpty() ? throwable.getClass().getSimpleName()
                : throwable.getClass().getSimpleName() + ": " + message;
    }

    private static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder escaped = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < MIN_PRINTABLE_CHAR) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.append('"').toString();
    }

    private static String pad(String label) {
        return label.length() >= LABEL_COLUMN_WIDTH ? label + ' ' : String.format("%-" + LABEL_COLUMN_WIDTH + "s",
                label);
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
