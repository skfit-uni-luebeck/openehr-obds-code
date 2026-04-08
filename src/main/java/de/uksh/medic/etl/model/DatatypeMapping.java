package de.uksh.medic.etl.model;

import java.util.List;

public class DatatypeMapping {

    private String datatype;
    private Object value;
    private List<DatatypeMapping> mappings;

    public DatatypeMapping(String datatype, Object value) {
        this.datatype = datatype;
        this.value = value;
    }

    public DatatypeMapping(String datatype, Object value, List<DatatypeMapping> mappings) {
        this.datatype = datatype;
        this.value = value;
        this.mappings = mappings;
    }

    public DatatypeMapping(Object value, List<DatatypeMapping> mappings) {
        this.value = value;
        this.mappings = mappings;
    }

    public String getDatatype() {
        return datatype;
    }

    public Object getValue() {
        return value;
    }

    public List<DatatypeMapping> getMappings() {
        return mappings;
    }

}
