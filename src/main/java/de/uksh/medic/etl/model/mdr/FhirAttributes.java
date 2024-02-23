package de.uksh.medic.etl.model.mdr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum for Attributes queried from MDR.
 */
public enum FhirAttributes {
    /**
     * system attribute in MDR.
     */
    SYSTEM("system"),
    /**
     * source attribute in MDR.
     */
    SOURCE("source"),
    /**
     * target attribute in MDR.
     */
    TARGET("target"),
    /**
     * ID attribute in MDR.
     */
    ID("id"),
    /**
     * conceptMap attribute in MDR.
     */
    CONCEPTMAP("conceptMap"),
    /**
     * code attribute in MDR.
     */
    CODE("code"),
    /**
     * version attribute in MDR.
     */
    VERSION("version"),
    /**
     * version attribute in MDR.
     */
    PATH("path");

    private final String label;

    FhirAttributes(String label) {
        this.label = label;
    }

    @Override
    @JsonValue
    public String toString() {
        return this.label;
    }

    /**
     * Look Enum up from String.
     * @param s string to be looked up
     * @return corresponding Enum
     */
    @JsonCreator
    public static FhirAttributes fromString(String s) {
        for (FhirAttributes a : FhirAttributes.values()) {
            if (a.label.equalsIgnoreCase(s)) {
                return a;
            }
        }
        return null;
    }

}
