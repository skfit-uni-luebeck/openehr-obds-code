package de.uksh.medic.etl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import de.uksh.medic.etl.jobs.FhirResolver;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrAttributes;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrConvert;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrItemSet;
import de.uksh.medic.etl.jobs.mdr.centraxx.CxxMdrLogin;
import de.uksh.medic.etl.model.MappingAttributes;
import de.uksh.medic.etl.model.mdr.centraxx.CxxItemSet;
import de.uksh.medic.etl.model.mdr.centraxx.RelationConvert;
import de.uksh.medic.etl.openehrmapper.EHRParser;
import de.uksh.medic.etl.settings.ConfigurationLoader;
import de.uksh.medic.etl.settings.CxxMdrSettings;
import de.uksh.medic.etl.settings.Mapping;
import de.uksh.medic.etl.settings.Settings;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import org.tinylog.Logger;
import org.xml.sax.SAXException;

public final class OpenEhrObds {

    private static final Map<String, Map<String, MappingAttributes>> FHIR_ATTRIBUTES = new HashMap<>();

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
            if (m.getSource() == null) {
                return;
            }
            CxxItemSet is = CxxMdrItemSet.get(Settings.getCxxmdr(), m.getTarget());
            is.getItems().forEach(it -> {
                try {
                    if (!FHIR_ATTRIBUTES.containsKey(m.getTarget())) {
                        FHIR_ATTRIBUTES.put(m.getTarget(), new HashMap<>());
                    }
                    FHIR_ATTRIBUTES.getOrDefault(m.getTarget(), new HashMap<>()).put(it.getId(),
                            CxxMdrAttributes.getAttributes(Settings.getCxxmdr(), m.getTarget(), "fhir", it.getId()));
                } catch (URISyntaxException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
        });

        XmlMapper xmlMapper = new XmlMapper();

        // ToDo: Replace with Kafka consumer

        File f = new File("st.xml");

        Map<String, Object> m = new LinkedHashMap<>();
        walkXmlTree(xmlMapper.readValue(f, new TypeReference<LinkedHashMap<String, Object>>() {
        }).entrySet(), 1, "", m);

        Logger.info("OpenEhrObds started!");
    }

    @SuppressWarnings({ "unchecked" })
    public static void walkXmlTree(Set<Map.Entry<String, Object>> xmlSet, int depth, String path,
            Map<String, Object> resMap) {

        if (depth > Settings.getDepthLimit()) {
            return;
        }
        if (Settings.getMapping().containsKey(path) && Settings.getMapping().get(path).getSource() != null) {

            Mapping m = Settings.getMapping().get(path);

            Map<String, Object> mapped = convertMdr(xmlSet, m);
            listConv(mapped);
            mapped.entrySet().forEach(e -> queryFhirTs(m, e));
            Map<String, Object> result = formatMap((Map<String, Object>) mapped);

            buildOpenEhrComposition(result);

            resMap.putAll(result);

        }

        xmlSet.forEach(entry -> {
            String newPath = path + "/" + entry.getKey();
            int newDepth = depth + 1;

            switch (entry.getValue()) {
                case @SuppressWarnings("rawtypes") Map h -> {
                    if (Settings.getMapping().containsKey(newPath)
                            && Settings.getMapping().get(newPath).getSplit()) {
                        Map<String, Object> m = new LinkedHashMap<>();
                        System.out.println(entry.getKey());
                        walkXmlTree(h.entrySet(), newDepth, newPath, m);
                        resMap.put(newPath, List.of(m));
                    } else {
                        walkXmlTree(h.entrySet(), newDepth, newPath, resMap);
                    }
                }
                case @SuppressWarnings("rawtypes") List a -> {

                    if (Settings.getMapping().containsKey(newPath)
                            && Settings.getMapping().get(newPath).getSplit()) {
                        List<Map<String, Object>> l = new ArrayList<>();
                        a.forEach(b -> {
                            Map<String, Object> m = new LinkedHashMap<>();
                            l.add(m);
                            System.out.println(entry.getKey());
                            walkXmlTree(((Map<String, Object>) b).entrySet(), newDepth, newPath, m);
                        });
                        resMap.put(newPath, l);
                    } else {
                        a.forEach(b -> {
                            System.out.println(entry.getKey());
                            walkXmlTree(((Map<String, Object>) b).entrySet(), newDepth, newPath, resMap);
                        });
                    }
                }

                default -> {
                }
            }
        });

    }

    private static void listConv(Map<String, Object> input) {
        input.entrySet().forEach(e -> {
            if (e.getValue() == null) {
                return;
            }
            switch (e.getValue()) {
                case List l -> {
                }
                default -> {
                    e.setValue(List.of(e.getValue()));
                }
            }
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void queryFhirTs(Mapping m, Map.Entry<String, Object> e) {
        if (e.getValue() == null) {
            return;
        }
        MappingAttributes fa = FHIR_ATTRIBUTES.get(m.getTarget()).get(e.getKey());
        List<Object> listed = new ArrayList<>();
        for (Object o : (List) e.getValue()) {
            if (fa != null && fa.getSystem() != null) {
                String code = switch (o) {
                    case String c -> c;
                    case Map map -> ((Map<String, String>) map).get("code");
                    default -> null;
                };
                if (fa.getConceptMap() == null) {
                    String version = switch (o) {
                        case String c -> fa.getVersion();
                        case Map map -> ((Map<String, String>) map).get("version");
                        default -> null;
                    };
                    listed.add(FhirResolver.lookUp(fa.getSystem(), version, code));
                } else if (fa.getConceptMap() != null) {
                    listed.add(FhirResolver.conceptMap(fa.getConceptMap(), fa.getId(), fa.getSource(),
                            fa.getTarget(), code));
                }
            } else {
                listed.add(o);
            }
        }
        e.setValue(listed);
    }

    private static Map<String, Object> formatMap(Map<String, Object> input) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Entry<String, Object> e : input.entrySet()) {
            ArrayList<String> al = new ArrayList<>();
            al.addAll(Arrays.asList(e.getKey().split("/")));
            splitMap(e.getValue(), al, out);
        }
        return out;
    }

    @SuppressWarnings({ "unchecked" })
    private static void splitMap(Object value, List<String> key, Map<String, Object> out) {
        if (key.size() > 1) {
            String k = key.removeFirst();
            Map<String, Object> m = (Map<String, Object>) out.getOrDefault(k,
                    new LinkedHashMap<>());
            out.put(k, m);
            splitMap(value, key, m);
        } else if (key.size() == 1) {
            out.put(key.removeFirst(), value);
        }
    }

    private static Map<String, Object> convertMdr(Set<Map.Entry<String, Object>> xmlSet, Mapping m) {
        RelationConvert conv = new RelationConvert();
        conv.setSourceProfileCode(m.getSource());
        conv.setTargetProfileCode(m.getTarget());
        conv.setSourceProfileVersion(m.getSourceVersion());
        conv.setTargetProfileVersion(m.getTargetVersion());
        conv.setValues(xmlSet.stream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
        try {
            return CxxMdrConvert.convert(Settings.getCxxmdr(), conv).getValues();
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private static void buildOpenEhrComposition(Map<String, Object> data) {
        EHRParser ep = new EHRParser();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.json"))) {
            writer.write(ep.build(data));
        } catch (XPathExpressionException | IOException | ParserConfigurationException | SAXException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
