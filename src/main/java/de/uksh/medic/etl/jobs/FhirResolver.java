package de.uksh.medic.etl.jobs;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import de.uksh.medic.etl.settings.Settings;
import java.net.URI;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.tinylog.Logger;

/**
 * Class to query a FHIR server for data.
 */
public final class FhirResolver {

    private static final FhirContext CTX = FhirContext.forR4();
    private static IGenericClient terminologyClient;

    private FhirResolver() {
    }

    /**
     * Initializes the class by creating clients for clinical data and terminology.
     */
    public static void initialize() {
        if (Settings.getFhirTsUrl() != null) {
            terminologyClient = CTX.newRestfulGenericClient(Settings.getFhirTsUrl().toString());
        }
    }

    public static Coding conceptMap(URI conceptMapUri, URI system, URI source, URI target, String input) {
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

        return new Coding();
    }

    public static Coding lookUp(URI system, String version, String code) {
        Parameters params = new Parameters();
        params.addParameter("system", new UriType(system));
        params.addParameter("code", code);
        params.addParameter("version", version);
        try {
            Parameters result = terminologyClient.operation().onType(CodeSystem.class)
                    .named("lookup").withParameters(params).execute();

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

            return coding;

        } catch (FhirClientConnectionException e) {
            Logger.error("Could not connect to FHIR Terminology Server", e);
        }

        return null;
    }

}
