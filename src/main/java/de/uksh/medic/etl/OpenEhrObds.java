package de.uksh.medic.etl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import de.uksh.medic.etl.jobs.FhirResolver;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrAttributes;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrConvert;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrItemSet;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrLogin;
import de.uksh.medic.etl.model.FhirAttributes;
import de.uksh.medic.etl.model.mdr.centraxx.CxxItemSet;
import de.uksh.medic.etl.model.mdr.centraxx.RelationConvert;
import de.uksh.medic.etl.settings.ConfigurationLoader;
import de.uksh.medic.etl.settings.CxxMdrSettings;
import de.uksh.medic.etl.settings.Mapping;
import de.uksh.medic.etl.settings.Settings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.tinylog.Logger;

public final class OpenEhrObds {

    private static final Map<String, Map<String, FhirAttributes>> ATTRIBUTE_CACHE = new HashMap<>();

    private OpenEhrObds() {
    }

    public static void main(String[] args) throws IOException {
        InputStream settingsYaml = ClassLoader.getSystemClassLoader().getResourceAsStream("settings.yml");
        if (args.length == 1) {
            settingsYaml = new FileInputStream(args[0]);
        }

        ConfigurationLoader configLoader = new ConfigurationLoader();
        configLoader.loadConfiguration(settingsYaml, Settings.class);

        FhirResolver.initalize();
        CxxMdrSettings mdrSettings = Settings.getCxxmdr();
        if (mdrSettings != null) {
            CxxMdrLogin.login(mdrSettings);
        }

        Settings.getMapping().values().forEach(m -> {
            CxxItemSet is = CxxMdrItemSet.get(Settings.getCxxmdr(), m.getTarget());
            is.getItems().forEach(it -> {
                try {
                    if (!ATTRIBUTE_CACHE.containsKey(m.getTarget())) {
                        ATTRIBUTE_CACHE.put(m.getTarget(), new HashMap<>());
                    }
                    ATTRIBUTE_CACHE.getOrDefault(m.getTarget(), new HashMap<>()).put(it.getId(),
                            CxxMdrAttributes.getAttributes(Settings.getCxxmdr(), m.getTarget(), it.getId()));
                } catch (URISyntaxException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
        });

        XmlMapper xmlMapper = new XmlMapper();

        // ToDo: Replace with Kafka consumer

        File f = new File("file_1705482057-clean.xml");

        walkXmlTree(xmlMapper.readValue(f, new TypeReference<LinkedHashMap<String, Object>>() {
        }).entrySet(), 1, "");

        Logger.info("OpenEhrObds started!");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void walkXmlTree(Set<Map.Entry<String, Object>> xmlSet, int depth, String path) {
        if (depth <= Settings.getDepthLimit()) {
            if (Settings.getMapping().containsKey(path)) {
                System.out.println("Mapping " + path);

                RelationConvert conv = new RelationConvert();
                Mapping m = Settings.getMapping().get(path);
                conv.setSourceProfileCode(m.getSource());
                conv.setTargetProfileCode(m.getTarget());
                conv.setSourceProfileVersion(m.getSourceVersion());
                conv.setTargetProfileVersion(m.getTargetVersion());
                conv.setValues(xmlSet.stream()
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));

                try {
                    RelationConvert res = CxxMdrConvert.convert(Settings.getCxxmdr(), conv);

                    res.getValues().entrySet().forEach(e -> {
                        queryFhirTs(m, e);
                    });

                    System.out.println(res);
                } catch (JsonProcessingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            xmlSet.forEach(entry -> {
                String newPath = path + "/" + entry.getKey();
                int newDepth = depth + 1;

                switch (entry.getValue()) {
                    case Map h -> {
                        walkXmlTree(h.entrySet(), newDepth, newPath);
                    }
                    case List a -> a.forEach(b -> walkXmlTree(((Map) b).entrySet(), newDepth, newPath));
                    default -> {
                    }
                }
            });
        }
    }

    private static void queryFhirTs(Mapping m, Map.Entry<String, Object> e) {
        FhirAttributes fa = ATTRIBUTE_CACHE.get(m.getTarget()).getOrDefault(e.getKey(), null);
        if (fa != null) {
            if (fa.getVersion() != null) {
                e.setValue(FhirResolver.lookUp(fa.getSystem(), fa.getVersion(), (String) e.getValue()));
            } else if (fa.getConceptMap() != null) {
                e.setValue(FhirResolver.conceptMap(fa.getConceptMap(), fa.getId(), fa.getSource(),
                        fa.getTarget(), (String) e.getValue()).getCode());
            }
        }
    }

}
