package de.uksh.medic.etl.settings;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URL;

/**
 * Settings for OpenEhrObds.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Settings {

    private static URL fhirTsUrl;
    private static CxxMdrSettings cxxmdr;

    private Settings() {}

    public static URL getFhirTsUrl() {
        return fhirTsUrl;
    }

    @JsonProperty("fhirTsUrl")
    public void setFhirTsUrl(URL newFhirTsUrl) {
        fhirTsUrl = newFhirTsUrl;
    }

    public static CxxMdrSettings getCxxmdr() {
        return cxxmdr;
    }

    @JsonProperty("cxxmdr")
    public void setCxxMdrSettings(CxxMdrSettings newCxxMdr) {
        cxxmdr = newCxxMdr;
    }

}
