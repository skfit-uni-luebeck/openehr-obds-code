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
    private static URL fhirServerUrl;
    private static URI openEhrUrl;
    private static String openEhrUser;
    private static String openEhrPassword;
    private static String mode;
    private static CxxMdrSettings cxxmdr;
    private static KafkaSettings kafka;
    private static Map<String, Mapping> mapping;
    private static int depthLimit;
    private static String systemId;

    private Settings() {}

    public static URL getFhirTsUrl() {
        return fhirTsUrl;
    }

    public static URL getFhirServerUrl() {
        return fhirServerUrl;
    }

    public static String getMode() {
        return mode;
    }

    public static String getSystemId() {
        return systemId;
    }

    @JsonProperty("systemId")
    public void setSystemId(String systemId) {
        Settings.systemId = systemId;
    }

    @JsonProperty("mode")
    public void setMode(String mode) {
        Settings.mode = mode;
    }

    @JsonProperty("fhirTsUrl")
    public void setFhirTsUrl(URL newFhirTsUrl) {
        fhirTsUrl = newFhirTsUrl;
    }

    @JsonProperty("fhirServerUrl")
    public void setFhirServerUrl(URL newFhirServerUrl) {
        fhirServerUrl = newFhirServerUrl;
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

    public static CxxMdrSettings getCxxmdr() {
        return cxxmdr;
    }

    @JsonProperty("cxxmdr")
    public void setCxxMdrSettings(CxxMdrSettings newCxxMdr) {
        cxxmdr = newCxxMdr;
    }

    public static KafkaSettings getKafka() {
        return kafka;
    }

    @JsonProperty("kafka")
    public void setKafkaSettings(KafkaSettings newKafka) {
        kafka = newKafka;
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
