package de.uksh.medic.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.uksh.medic.etl.ServerAvailability.ServerKind;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the availability checks of the openEHR server and the two FHIR servers.
 */
class ServerAvailabilityTest {

    private static final String OPENEHR_URL = "https://ehr.example.org/ehr/";
    private static final String FHIR_SERVER_URL = "https://fhir.example.org/fhir/";
    private static final String FHIR_TS_URL = "https://ontoserver.example.org/fhir/";
    /** A probe verdict meaning "the server answered with HTTP 2xx" (null means no failure). */
    private static final String NO_FAILURE = null;
    /** A probe verdict meaning "the server answered with a non-2xx status". */
    private static final String FORBIDDEN = "HTTP 403";
    private static final int SINGLE_ATTEMPT = 1;
    private static final int THREE_ATTEMPTS = 3;
    private static final int PROBE_TIMEOUT_MS = 500;
    private static final int FAST_PROBE_TIMEOUT_MS = 100;
    private static final int SLEEP_MS = 1000;
    private static final int CHECK_INTERVAL_MS = 10;
    private static final int NO_WARNING_MS = 0;
    private static final int LONG_CACHE_MS = 60000;
    private static final int OUTAGE_AFTER_CALLS = 2;

    /**
     * Start every test with an empty registry and fast probes.
     */
    @BeforeEach
    void setUp() {
        ServerAvailability.reset();
        ServerAvailability.configure(PROBE_TIMEOUT_MS, SINGLE_ATTEMPT);
    }

    @Test
    void nothingConfiguredIsAlwaysAvailable() {
        assertTrue(ServerAvailability.allAvailable());
        ServerAvailability.awaitServers(CHECK_INTERVAL_MS, NO_WARNING_MS);
        assertTrue(ServerAvailability.allAvailable());
        assertEquals("none", ServerAvailability.describeUnavailable());
    }

    @Test
    void positiveVerdictIsCachedWithinInterval() {
        AtomicInteger probes = registerAllThreeServers(() -> NO_FAILURE);

        ServerAvailability.awaitServers(LONG_CACHE_MS, NO_WARNING_MS);
        ServerAvailability.awaitServers(LONG_CACHE_MS, NO_WARNING_MS);

        assertEquals(3, probes.get(), "each of the three servers must be probed exactly once");
    }

    @Test
    void processingWaitsUntilTheServerIsAvailableAgain() {
        AtomicInteger calls = new AtomicInteger();
        ServerAvailability.register(ServerKind.FHIR_TERMINOLOGY_SERVER, FHIR_TS_URL, () -> {
            if (calls.incrementAndGet() <= OUTAGE_AFTER_CALLS) {
                return FORBIDDEN;
            }
            return NO_FAILURE;
        });

        ServerAvailability.awaitServers(CHECK_INTERVAL_MS, NO_WARNING_MS);

        assertTrue(ServerAvailability.isFhirTsAvailable());
        assertTrue(calls.get() > OUTAGE_AFTER_CALLS, "the server must be probed again until it answers");
        assertTrue(ServerAvailability.allAvailable());
    }

    @Test
    void anUnavailableServerIsNamedWithItsUrlAndReason() {
        registerAllThreeServers(() -> NO_FAILURE);
        ServerAvailability.register(ServerKind.FHIR_TERMINOLOGY_SERVER, FHIR_TS_URL, () -> FORBIDDEN);

        assertFalse(ServerAvailability.checkAll());

        String unavailable = ServerAvailability.describeUnavailable();
        assertTrue(unavailable.contains("FHIR Terminology Server"), unavailable);
        assertTrue(unavailable.contains(FHIR_TS_URL), unavailable);
        assertTrue(unavailable.contains(FORBIDDEN), unavailable);
        assertFalse(unavailable.contains("FHIR Server ["), unavailable);

        String available = ServerAvailability.describeAvailable();
        assertTrue(available.contains("openEHR server"), available);
        assertTrue(available.contains("FHIR Server ["), available);
        assertFalse(available.contains("FHIR Terminology Server"), available);

        assertTrue(ServerAvailability.isFhirServerAvailable());
        assertTrue(ServerAvailability.isOpenEhrAvailable());
        assertFalse(ServerAvailability.isFhirTsAvailable());
    }

    @Test
    void aSlowServerIsReportedAsUnavailableInsteadOfBlockingForever() {
        ServerAvailability.configure(FAST_PROBE_TIMEOUT_MS, SINGLE_ATTEMPT);
        ServerAvailability.register(ServerKind.OPENEHR_SERVER, OPENEHR_URL, () -> {
            sleep(SLEEP_MS);
            return NO_FAILURE;
        });

        assertFalse(ServerAvailability.checkAll());
        assertTrue(ServerAvailability.describeUnavailable().contains("timed out"));
    }

    @Test
    void transientFailuresAreRetriedBeforeDeclaringUnavailable() {
        AtomicInteger attempts = new AtomicInteger();
        ServerAvailability.configure(PROBE_TIMEOUT_MS, THREE_ATTEMPTS);
        ServerAvailability.register(ServerKind.FHIR_SERVER, FHIR_SERVER_URL, () -> {
            if (attempts.incrementAndGet() < THREE_ATTEMPTS) {
                return "HTTP 502";
            }
            return NO_FAILURE;
        });

        assertTrue(ServerAvailability.checkAll());
        assertEquals(THREE_ATTEMPTS, attempts.get());
        assertTrue(ServerAvailability.isFhirServerAvailable());
    }

    @Test
    void onlyHttpTwoXxCountsAsAvailable() {
        assertTrue(ServerAvailability.isHttpSuccess(200));
        assertTrue(ServerAvailability.isHttpSuccess(204));
        assertFalse(ServerAvailability.isHttpSuccess(300));
        assertFalse(ServerAvailability.isHttpSuccess(401));
        assertFalse(ServerAvailability.isHttpSuccess(403));
        assertFalse(ServerAvailability.isHttpSuccess(404));
        assertFalse(ServerAvailability.isHttpSuccess(500));
    }

    @Test
    void markingAServerKeepsTheOtherServersUntouched() {
        registerAllThreeServers(() -> NO_FAILURE);
        assertTrue(ServerAvailability.checkAll());

        ServerAvailability.markFhirTsUnavailable("connection error: reset");

        assertFalse(ServerAvailability.isFhirTsAvailable());
        assertTrue(ServerAvailability.isFhirServerAvailable());
        assertTrue(ServerAvailability.isOpenEhrAvailable());
        assertFalse(ServerAvailability.allAvailable());
    }

    @Test
    void statusJsonDescribesAllThreeServers() throws Exception {
        registerAllThreeServers(() -> NO_FAILURE);

        JsonNode servers = new ObjectMapper().readTree(ServerAvailability.statusJson());

        assertEquals(3, servers.size());
        assertNotNull(servers.get("openEhrServer"));
        assertNotNull(servers.get("fhirServer"));
        assertNotNull(servers.get("fhirTerminologyServer"));
        assertEquals("unknown", servers.get("fhirTerminologyServer").get("state").asText());
        assertEquals(OPENEHR_URL, servers.get("openEhrServer").get("url").asText());
        assertEquals(FHIR_SERVER_URL, servers.get("fhirServer").get("url").asText());
        assertEquals(FHIR_TS_URL, servers.get("fhirTerminologyServer").get("url").asText());

        assertTrue(ServerAvailability.checkAll());
        assertEquals("ok", new ObjectMapper().readTree(ServerAvailability.statusJson())
                .get("fhirTerminologyServer").get("state").asText());
    }

    @Test
    void statusJsonReportsTheReasonOfTheOutage() throws Exception {
        registerAllThreeServers(() -> NO_FAILURE);
        ServerAvailability.register(ServerKind.FHIR_SERVER, FHIR_SERVER_URL, () -> FORBIDDEN);
        ServerAvailability.checkAll();

        JsonNode fhirServer = new ObjectMapper().readTree(ServerAvailability.statusJson()).get("fhirServer");

        assertEquals("down", fhirServer.get("state").asText());
        assertFalse(fhirServer.get("available").asBoolean());
        assertEquals(FORBIDDEN, fhirServer.get("lastError").asText());
        assertTrue(fhirServer.get("lastCheckEpochMs").asLong() > 0);
    }

    @Test
    void anUnconfiguredServerIsReportedAndNeverBlocksProcessing() throws Exception {
        ServerAvailability.register(ServerKind.OPENEHR_SERVER, OPENEHR_URL, () -> NO_FAILURE);

        JsonNode servers = new ObjectMapper().readTree(ServerAvailability.statusJson());

        assertEquals("not-configured", servers.get("fhirServer").get("state").asText());
        assertFalse(servers.get("fhirServer").get("configured").asBoolean());
        ServerAvailability.awaitServers(CHECK_INTERVAL_MS, NO_WARNING_MS);
        assertTrue(ServerAvailability.allAvailable());
    }

    private static AtomicInteger registerAllThreeServers(ServerAvailability.Probe probe) {
        AtomicInteger probes = new AtomicInteger();
        ServerAvailability.Probe counting = () -> {
            probes.incrementAndGet();
            return probe.check();
        };
        ServerAvailability.register(ServerKind.OPENEHR_SERVER, OPENEHR_URL, counting);
        ServerAvailability.register(ServerKind.FHIR_SERVER, FHIR_SERVER_URL, counting);
        ServerAvailability.register(ServerKind.FHIR_TERMINOLOGY_SERVER, FHIR_TS_URL, counting);
        return probes;
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
