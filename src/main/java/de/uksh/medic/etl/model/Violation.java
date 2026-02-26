package de.uksh.medic.etl.model;

import java.util.List;

public class Violation {

    private Object value;
    private Object lower;
    private Object upper;
    private String unit = "";

    public Violation(Object value, Object lower, Object upper) {
        this.value = value;
        this.lower = lower;
        this.upper = upper;
    }

    public Violation(Object value, Object lower, Object upper, String unit) {
        this.value = value;
        this.lower = lower;
        this.upper = upper;
        this.unit = unit;
    }

    public Object getValue() {
        return value;
    }

    public Object getLower() {
        return lower;
    }

    public Object getUpper() {
        return upper;
    }

    public String getUnit() {
        return unit;
    }

    @Override
    public String toString() {
        return String.join(";", List.of(value.toString(), lower.toString(), upper.toString(), unit));
    }

}
