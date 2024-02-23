package de.uksh.medic.etl.model.mdr.centraxx;

import de.uksh.medic.etl.model.mdr.FhirAttributes;
import java.util.List;

/**
 * Model for AttributeValue in Kairos CentraXX MDR.
 */
public class CxxAttributeValue {

    private String domain;
    private FhirAttributes attribute;
    private String value;
    private List<CxxLinks> links;

    public String getDomain() {
        return domain;
    }

    public FhirAttributes getAttribute() {
        return attribute;
    }

    public String getValue() {
        return value;
    }

    public List<CxxLinks> getLinks() {
        return links;
    }

}
