package de.uksh.medic.etl.settings;

/**
 * Defines mappings between source and target profiles used for MDR mapping.
 */
public class Mapping {

    private String source;
    private String sourceVersion;
    private String target;
    private String targetVersion;
    private boolean split;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    public boolean getSplit() {
        return split;
    }

    public void setSplit(boolean split) {
        this.split = split;
    }

}
