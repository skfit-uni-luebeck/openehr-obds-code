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

    private static FhirContext ctx = FhirContext.forR4();
    private static IGenericClient terminologyClient;

    private FhirResolver() {
    }

    /**
     * Initializes the class by creating clients for clinical data and terminology.
     */
    public static void initalize() {
        if (Settings.getFhirTsUrl() != null) {
            terminologyClient = ctx.newRestfulGenericClient(Settings.getFhirTsUrl().toString());
        }
    }

    public static Coding conceptMap(URI conceptMapUri, String conceptMapId, URI source, URI target, String input) {
        Parameters params = new Parameters();
        params.addParameter("system", new UriType(source));
        params.addParameter("source", new UriType(source));
        params.addParameter("target", new UriType(target));
        params.addParameter("conceptMap",
                new UriType(conceptMapUri));
        params.addParameter("code", input);
        try {
            Parameters result = terminologyClient.operation()
                    .onInstance("ConceptMap/" + conceptMapId)
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

}
