package de.uksh.medic.etl.settings;

/**
 * Defines mappings between source and target profiles used for MDR mapping.
 */
public class Mapping {

    private String source;
    private int sourceVersion;
    private String target;
    private int targetVersion;
    private boolean split;
    private String templateId;
    private boolean uniqueComposition = false;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(int sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public int getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(int targetVersion) {
        this.targetVersion = targetVersion;
    }

    public boolean getSplit() {
        return split;
    }

    public void setSplit(boolean split) {
        this.split = split;
    }

    public String getTemplateId() {
        return templateId;
    }

    public void setTemplateId(String templateId) {
        this.templateId = templateId;
    }

    public boolean isUniqueComposition() {
        return uniqueComposition;
    }

    public void setUniqueComposition(boolean uniqueComposition) {
        this.uniqueComposition = uniqueComposition;
    }
}
