package de.uksh.medic.etl.settings;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.net.URL;
import java.util.Map;

/**
 * Settings for OpenEhrObds.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Settings {

    private static URL fhirTsUrl;
    private static URI openEhrUrl;
    private static String openEhrUser;
    private static String openEhrPassword;
    private static URI xdsUrl;
    private static String target;
    private static CxxMdrSettings cxxmdr;
    private static Map<String, Mapping> mapping;
    private static int depthLimit;

    private Settings() {}

    public static URL getFhirTsUrl() {
        return fhirTsUrl;
    }

    @JsonProperty("fhirTsUrl")
    public void setFhirTsUrl(URL newFhirTsUrl) {
        fhirTsUrl = newFhirTsUrl;
    }

    public static URI getOpenEhrUrl() {
        return openEhrUrl;
    }

    @JsonProperty("openEhrUrl")
    public void setOpenEhrUrl(URI newOpenEhrUrl) {
        openEhrUrl = newOpenEhrUrl;
    }

    public static String getOpenEhrUser() {
        return openEhrUser;
    }

    @JsonProperty("openEhrUser")
    public void setOpenEhrUser(String newOpenEhrUser) {
        openEhrUser = newOpenEhrUser;
    }

    public static String getOpenEhrPassword() {
        return openEhrPassword;
    }

    @JsonProperty("openEhrPassword")
    public void setOpenEhrPassword(String newOpenEhrPassword) {
        openEhrPassword = newOpenEhrPassword;
    }

    public static URI getXdsUrl() {
        return xdsUrl;
    }

    @JsonProperty("xdsUrl")
    public void setXdsUrl(URI newXdsUrl) {
        xdsUrl = newXdsUrl;
    }

    public static String getTarget() {
        return target;
    }

    @JsonProperty("target")
    public void setTarget(String newTarget) {
        target = newTarget;
    }

    public static CxxMdrSettings getCxxmdr() {
        return cxxmdr;
    }

    @JsonProperty("cxxmdr")
    public void setCxxMdrSettings(CxxMdrSettings newCxxMdr) {
        cxxmdr = newCxxMdr;
    }

    public static Map<String, Mapping> getMapping() {
        return mapping;
    }

    @JsonProperty("mapping")
    public void setMapping(Map<String, Mapping> newMapping) {
        mapping = newMapping;
    }

    public static int getDepthLimit() {
        return depthLimit;
    }

    @JsonProperty("depthLimit")
    public void setDepthLimit(int newDepthLimit) {
        depthLimit = newDepthLimit;
    }

}
