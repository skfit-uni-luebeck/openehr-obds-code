package de.uksh.medic.etl.jobs.mdr.centraxx;

import com.google.common.base.Strings;
import de.uksh.medic.etl.jobs.FhirResolver;
import de.uksh.medic.etl.model.MappingAttributes;
import de.uksh.medic.etl.settings.CxxMdrSettings;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.utilities.CSVReader;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestTemplate;

public final class CxxMdrUnitConvert {

    private CxxMdrUnitConvert() {
    }

    /**
     * Converts data from a source profile to a target profile using Kairos CentraXX MDR.
     * @param mdr Configuration for MDR.
     * @param map Map with the current magnitude and unit
     * @param ma MappingAttributes object
     * @return Converted value or null if an error occured
     */
    public static String[] convert(CxxMdrSettings mdr, Map<String, String> map, MappingAttributes ma) {
        if (!map.containsKey("magnitude") || !map.containsKey("unit")
                || map.get("magnitude") == null || map.get("unit") == null
                || map.get("magnitude").isBlank() || map.get("unit").isBlank()) {
            return null;
        }

        String convertedUnit = null;

        if (ma.getConceptMap() != null && ma.getTarget() != null && ma.getSystem() != null && ma.getSource() != null) {
            Coding coding = FhirResolver.conceptMap(ma.getConceptMap(), ma.getSystem(), ma.getSource(),
                    ma.getTarget(), map.get("unit"));
            if (coding == null || coding.getCode() == null || coding.getCode().isBlank()) {
                return null;
            }
            convertedUnit = coding.getCode();
        }

        if (convertedUnit == null) {
            return null;
        }

        if (Strings.isNullOrEmpty(ma.getUnit())) {
            String[] ret = {map.get("magnitude"), convertedUnit};
            return ret;
        }

        RestTemplate rt = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, "text/csv");
        String response = rt.postForObject(mdr.getUrl() + "/rest/v1/units/convert",
                new HttpEntity<>(map.get("magnitude") + "," + convertedUnit + "," + ma.getUnit(), headers),
                String.class);
        if (response == null || response.isBlank()) {
            return null;
        }
        try {
            CSVReader reader = new CSVReader(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)));
            reader.readHeaders();
            reader.line();
            String[] ret = {reader.cell("targetValue"), ma.getUnit()};
            return ret;
        } catch (IOException ignored) { }
        return null;
    }
}
