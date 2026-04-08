package de.uksh.medic.etl.model;

public class DatatypeMapping {

    private String datatype;
    private Object value;

    public DatatypeMapping(String datatype, Object value) {
        this.datatype = datatype;
        this.value = value;
    }

    public String getDatatype() {
        return datatype;
    }

    public Object getValue() {
        return value;
    }

}
