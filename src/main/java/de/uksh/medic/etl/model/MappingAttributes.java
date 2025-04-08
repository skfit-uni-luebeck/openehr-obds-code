package de.uksh.medic.etl.model;

import java.net.URI;

public class MappingAttributes {

    private URI system;
    private URI source;
    private URI target;
    private URI conceptMap;
    private String id;
    private String code;
    private String version;
    private String unit;
    private String updateAql;
    private String deleteAql;

    public MappingAttributes() {
    }

    public URI getSystem() {
        return system;
    }

    public void setSystem(URI system) {
        this.system = system;
    }

    public URI getSource() {
        return source;
    }

    public void setSource(URI source) {
        this.source = source;
    }

    public URI getTarget() {
        return target;
    }

    public void setTarget(URI target) {
        this.target = target;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public URI getConceptMap() {
        return conceptMap;
    }

    public void setConceptMap(URI conceptMap) {
        this.conceptMap = conceptMap;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getUpdateAql() {
        return updateAql;
    }

    public void setUpdateAql(String updateAql) {
        this.updateAql = updateAql;
    }

    public String getDeleteAql() {
        return deleteAql;
    }

    public void setDeleteAql(String deleteAql) {
        this.deleteAql = deleteAql;
    }
}
