package de.uksh.medic.etl;

import com.nedap.archie.rm.datatypes.CodePhrase;
import com.nedap.archie.rm.datavalues.DvCodedText;
import com.nedap.archie.rm.datavalues.TermMapping;
import com.nedap.archie.rm.support.identification.TerminologyId;
import de.uksh.medic.etl.jobs.FhirResolver;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrUnitConvert;
import de.uksh.medic.etl.model.MappingAttributes;
import de.uksh.medic.etl.settings.Mapping;
import de.uksh.medic.etl.settings.Settings;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.hl7.fhir.r4.model.Coding;
import org.tinylog.Logger;

public final class FhirUtils {

    private FhirUtils() {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static void queryFhirTs(Map<String, Map<String, MappingAttributes>> fhirAttributes, Mapping m,
            Entry<String, Object> e, FhirResolver fr) {
        if (e.getValue() == null) {
            return;
        }
        MappingAttributes fa = fhirAttributes.get(m.getTarget()).get(e.getKey());
        List<Object> listed = new ArrayList<>();
        for (Object o : (List) e.getValue()) {
            if (fa != null && fa.getTarget() != null && "http://unitsofmeasure.org".equals(fa.getTarget().toString())
                    && fa.getConceptMap() != null) {
                switch (o) {
                    case String c -> listed.add(o);
                    case Map map when map.containsKey("magnitude") && map.containsKey("unit") -> {
                        String[] newMagnitude = CxxMdrUnitConvert.convert(fr, Settings.getCxxmdr(), map, fa);
                        if (newMagnitude != null) {
                            map.replace("unit", newMagnitude[1]);
                            map.replace("magnitude", newMagnitude[0]);
                            listed.add(new String[] {newMagnitude[0], newMagnitude[1]});
                        } else {
                            Logger.error("Could not convert unit");
                            e.setValue(null);
                            return;
                        }
                    }
                    default -> {
                    }
                }
            } else if (fa != null && fa.getSystem() != null) {
                String code = switch (o) {
                    case String c -> c;
                    case Map map -> ((Map<String, String>) map).get("code");
                    default -> null;
                };
                if (fa.getConceptMap() == null) {
                    String version = switch (o) {
                        case String ignored -> fa.getVersion();
                        case Map map -> ((Map<String, String>) map).get("version");
                        default -> null;
                    };
                    listed.add(fr.lookUp(fa.getSystem(), version, code));
                } else if (fa.getConceptMap() != null) {
                    Coding c = fr.conceptMap(fa.getConceptMap(), fa.getSystem(), fa.getSource(), fa.getTarget(), code);
                    DvCodedText ct = new DvCodedText();
                    ct.setDefiningCode(new CodePhrase(
                            new TerminologyId(c.getSystem(), c.getVersion()),
                            c.getCode(), c.getDisplay()));
                    ct.setValue(c.getDisplay());
                    TermMapping tm = new TermMapping();
                    tm.setTarget(new CodePhrase(new TerminologyId(fa.getSystem().toString()), code));
                    tm.setMatch('?');
                    ct.addMapping(tm);
                    listed.add(ct);
                }
            } else {
                listed.add(o);
            }
        }
        e.setValue(listed);
    }

}
