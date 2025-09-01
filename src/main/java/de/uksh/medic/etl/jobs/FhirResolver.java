package de.uksh.medic.etl.jobs;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.client.interceptor.AdditionalRequestHeadersInterceptor;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import de.uksh.medic.etl.settings.Settings;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.tinylog.Logger;

/**
 * Class to query a FHIR server for data.
 */
public final class FhirResolver {

    private static final FhirContext CTX = FhirContext.forR4();
    private static final Map<String, Coding> CACHE_LOOKUP = new HashMap<>();
    private static final Map<String, Coding> CACHE_CONCEPTMAP = new HashMap<>();
    private static IGenericClient terminologyClient;

    /**
     * Initializes the class by creating clients for clinical data and terminology.
     */
    public FhirResolver() {
        if (Settings.getFhirTsUrl() != null) {
            String topicName = (Settings.getKafka() != null && Settings.getKafka().getReadTopic() != null
                    && !"".equals(Settings.getKafka().getReadTopic())) ? Settings.getKafka().getReadTopic()
                            : "JVM-ETL Dev";
            terminologyClient = CTX.newRestfulGenericClient(Settings.getFhirTsUrl().toString());
            AdditionalRequestHeadersInterceptor interceptor = new AdditionalRequestHeadersInterceptor();
            interceptor.addHeaderValue("User-Agent", topicName);
            terminologyClient.registerInterceptor(interceptor);
        }
    }

    public Coding conceptMap(URI conceptMapUri, URI system, URI source, URI target, String input) {
        String key = String.join("|", conceptMapUri.toString(), system.toString(), source.toString(), target.toString(),
                input);
        CACHE_CONCEPTMAP.putIfAbsent(key, conceptMapServer(conceptMapUri, system, source, target, input));
        return CACHE_CONCEPTMAP.get(key);
    }

    public Coding conceptMapServer(URI conceptMapUri, URI system, URI source, URI target, String input) {
        Parameters params = new Parameters();
        params.addParameter("system", new UriType(system));
        params.addParameter("source", new UriType(source));
        params.addParameter("target", new UriType(target));
        params.addParameter("code", new CodeType(input));
        try {
            Parameters result = terminologyClient.operation()
                    .onType("ConceptMap")
                    .named("translate").withParameters(params).execute();

            for (ParametersParameterComponent p : result.getParameter()) {
                if (!p.getName().equals("match")) {
                    continue;
                }
                Coding coding = null;
                String str = null;
                for (ParametersParameterComponent c : p.getPart()) {
                    if (c.getValue() instanceof Coding) {
                        coding = (Coding) c.getValue();
                    }
                    if (c.getValue() instanceof StringType && c.getName().equals("source")) {
                        str = ((StringType) c.getValue()).getValue();
                    }
                }
                if (str != null && str.equals(conceptMapUri.toString())) {
                    return coding;
                }
            }

        } catch (FhirClientConnectionException e) {
            Logger.error("Could not connect to FHIR Terminology Server", e);
        }

        Logger.warn("Could not map code %s from %s to %s", input, source, target);
        return null;
    }

    public Coding lookUp(URI system, String version, String code) {
        String key = String.join("|", system.toString(), version, code);
        CACHE_LOOKUP.putIfAbsent(key, lookUpServer(system, version, code));
        return CACHE_LOOKUP.get(key);
    }

    public Coding lookUpServer(URI system, String version, String code) {
        Parameters params = new Parameters();
        params.addParameter("system", new UriType(system));
        params.addParameter("code", code);
        params.addParameter("version", version);
        try {
            Parameters result = terminologyClient.operation().onType(CodeSystem.class)
                    .named("lookup").withParameters(params).useHttpGet().execute();

            Coding coding = new Coding();
            for (ParametersParameterComponent p : result.getParameter()) {
                switch (p.getName()) {
                    case "display" -> coding.setDisplayElement((StringType) p.getValue());
                    case "version" -> coding.setVersionElement((StringType) p.getValue());
                    case "system" -> coding.setSystemElement((UriType) p.getValue());
                    case "code" -> coding.setCodeElement((CodeType) p.getValue());
                    default -> {
                    }
                }
            }

            if (version == null) {
                return coding.setVersion(null);
            }

            return coding;

        } catch (FhirClientConnectionException e) {
            Logger.error("Could not connect to FHIR Terminology Server", e);
        } catch (ResourceNotFoundException e) {
            Logger.error("Could not look up because system was not found.");
        }

        return null;
    }

}
