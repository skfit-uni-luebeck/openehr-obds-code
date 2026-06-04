package de.uksh.medic.etl;

import ca.uhn.fhir.rest.client.api.IGenericClient;
import de.uksh.medic.etl.jobs.FhirResolver;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.ehrbase.openehr.sdk.client.openehrclient.defaultrestclient.DefaultRestClient;

public final class Mapper {

    private Mapper() {
    }

    @SuppressWarnings({ "HiddenField" })
    public static Object javaMap(Set<Entry<String, Object>> xmlSet, String path, IGenericClient fhirClient,
            FhirResolver fhirResolver, DefaultRestClient openEhrClient, UtilMethods utils, Map<String, Object> cache) {

        return null;

    }

}
