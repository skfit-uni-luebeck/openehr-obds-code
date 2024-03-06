package de.uksh.medic.etl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.nedap.archie.json.JacksonUtil;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.datavalues.DvText;
import com.nedap.archie.rm.ehr.EhrStatus;
import com.nedap.archie.rm.generic.PartySelf;
import com.nedap.archie.rm.support.identification.HierObjectId;
import com.nedap.archie.rm.support.identification.PartyRef;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import org.apache.xmlbeans.XmlOptions;
import org.ehrbase.openehr.sdk.client.openehrclient.OpenEhrClientConfig;
import org.ehrbase.openehr.sdk.client.openehrclient.defaultrestclient.DefaultRestClient;
import org.ehrbase.openehr.sdk.generator.commons.aql.query.Query;
import org.ehrbase.openehr.sdk.response.dto.QueryResponseData;
import org.ehrbase.openehr.sdk.webtemplate.model.WebTemplate;
import org.ehrbase.openehr.sdk.webtemplate.parser.OPTParser;
import org.openehr.schemas.v1.OPERATIONALTEMPLATE;
import org.tinylog.Logger;
import org.xml.sax.SAXException;

public final class OpenEhrObds {

    private static final Map<String, Map<String, MappingAttributes>> FHIR_ATTRIBUTES = new HashMap<>();
    private static Integer i = 0;

    private OpenEhrObds() {
    }

    public static void main(String[] args) throws IOException {
        InputStream settingsYaml = ClassLoader.getSystemClassLoader().getResourceAsStream("settings.yml");
        if (args.length == 1) {
            settingsYaml = new FileInputStream(args[0]);
        }

        ConfigurationLoader configLoader = new ConfigurationLoader();
        configLoader.loadConfiguration(settingsYaml, Settings.class);

        FhirResolver.initialize();
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

        File f = new File("syst.xml");

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

        boolean split = Settings.getMapping().containsKey(path)
                && Settings.getMapping().get(path).getSplit();

        Map<String, Object> theMap = resMap;

        if (Settings.getMapping().containsKey(path) && Settings.getMapping().get(path).getSource() != null) {

            Mapping m = Settings.getMapping().get(path);

            Map<String, Object> mapped = convertMdr(xmlSet, m);
            assert mapped != null;
            mapped.values().removeIf(Objects::isNull);
            listConv(mapped);
            mapped.entrySet().forEach(e -> queryFhirTs(m, e));
            Map<String, Object> result = formatMap(mapped);

            result.putAll(resMap);
            theMap = result;

            if (split) {
                buildOpenEhrComposition(m.getTemplateId(), result);
            }

        }

        for (Map.Entry<String, Object> entry : xmlSet) {
            String newPath = path + "/" + entry.getKey();
            int newDepth = depth + 1;

            switch (entry.getValue()) {
                case @SuppressWarnings("rawtypes") Map h -> {
                    walkXmlTree(h.entrySet(), newDepth, newPath, theMap);
                }
                case @SuppressWarnings("rawtypes") List a -> {
                    for (Object b : a) {
                        System.out.println(entry.getKey());
                        walkXmlTree(((Map<String, Object>) b).entrySet(), newDepth, newPath, theMap);
                    }
                }
                default -> {
                }
            }
        }

    }

    private static void listConv(Map<String, Object> input) {
        input.entrySet().forEach(e -> {
            if (e.getValue() == null || e.getValue() instanceof List) {
                return;
            }
            List<Object> l = new ArrayList<>();
            l.add(e.getValue());
            e.setValue(l);
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
            ArrayList<String> al = new ArrayList<>(Arrays.asList(e.getKey().split("/")));
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

    @SuppressWarnings("unchecked")
    private static void buildOpenEhrComposition(String templateId, Map<String, Object> data) {

        WebTemplate template = null;
        String xml = null;
        URI ehrBaseUrl = Settings.getOpenEhrUrl();

        if (Settings.getOpenEhrUser() != null && Settings.getOpenEhrPassword() != null) {
            String credentials = ehrBaseUrl.toString();
            try {
                ehrBaseUrl = new URI(credentials.replace("://",
                        "://" + Settings.getOpenEhrUser() + ":" + Settings.getOpenEhrPassword() + "@"));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }

        DefaultRestClient client = new DefaultRestClient(new OpenEhrClientConfig(ehrBaseUrl));
        Optional<OPERATIONALTEMPLATE> oTemplate = client.templateEndpoint().findTemplate(templateId);
        assert oTemplate.isPresent();
        XmlOptions opts = new XmlOptions();
        opts.setSaveSyntheticDocumentElement(new QName("http://schemas.openehr.org/v1", "template"));
        xml = oTemplate.get().xmlText(opts);
        template = new OPTParser(oTemplate.get()).parse();

        EHRParser ep = new EHRParser();
        String ehr = "";
        Composition composition = null;

        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(i + "_" + ((List<String>) data.get("ehr_id")).getFirst() + ".json"))) {

            // Write JSON to file
            composition = ep.build(xml, data);

            System.out.println("Finished JSON-Generation. Generating String.");

            ehr = JacksonUtil.getObjectMapper().writeValueAsString(composition);
            writer.write(ehr);

        } catch (XPathExpressionException | IOException | ParserConfigurationException | SAXException
                | JAXBException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if ("raw".equals(Settings.getTarget())) {
            QueryResponseData ehrIds = client.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                    "SELECT e/ehr_id/value as EHR_ID FROM EHR e WHERE e/ehr_status/subject/external_ref/id/value = '"
                            + ((List<String>) data.get("ehr_id")).getFirst() + "'"));
            UUID ehrId = null;
            if (ehrIds.getColumns().size() == 0) {
                EhrStatus es = new EhrStatus();
                es.setArchetypeNodeId("openEHR-EHR-EHR_STATUS.generic.v1");
                es.setName(new DvText("EHR status"));
                es.setQueryable(true);
                es.setModifiable(true);
                es.setSubject(new PartySelf(new PartyRef(
                        new HierObjectId(((List<String>) data.get("ehr_id")).getFirst()), "DEMOGRAPHIC", "PERSON")));
                ehrId = client.ehrEndpoint().createEhr(es);
            } else if (ehrIds.getColumns().size() == 1) {
                ehrId = UUID.fromString((String) ehrIds.getRows().getFirst().getFirst());
            }
            client.compositionEndpoint(ehrId).mergeRaw(composition);
        }

        if ("xds".equals(Settings.getTarget())) {
            // // create flat json
            // FlatJsonMarshaller fjm = new FlatJsonMarshaller();
            // ehr = fjm.toFlatJson(composition, template);

            // // Convert to TDD

            // RestTemplate rt = new RestTemplate();
            // if (Settings.getOpenEhrUser() != null || Settings.getOpenEhrPassword() !=
            // null) {
            // rt.getInterceptors().add(
            // new BasicAuthenticationInterceptor(Settings.getOpenEhrUser(),
            // Settings.getOpenEhrPassword()));
            // }
            // MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            // form.set("templateId", templateId);
            // form.set("format", "FLAT");
            // UriComponentsBuilder builder = UriComponentsBuilder
            // .fromHttpUrl(Settings.getOpenEhrUrl() + "rest/v1/composition/convert/tdd");
            // builder.queryParams(form);

            // HttpHeaders headers = new HttpHeaders();
            // headers.set("Content-Type", "application/json");
            // headers.set("Accept", "application/xml");

            // String tdd = rt.postForObject(builder.build().encode().toUri(), new
            // HttpEntity<>(ehr, headers),
            // String.class);

            // XDS Envelope

            try (InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("iti41.xml")) {
                assert is != null;
                String content = new String(is.readAllBytes());
                content = content.replaceAll("MPIID", ((List<String>) data.get("ehr_id")).getFirst());
                content = content.replaceAll("EHRCONTENT", new String(Base64.getEncoder().encode(ehr.getBytes())));
                content = content.replace("UUID1", UUID.randomUUID().toString());
                content = content.replace("UUID2", UUID.randomUUID().toString());
                content = content.replace("TIMESTAMP", String.valueOf(System.currentTimeMillis()));
                content = content.replace("DATETIME",
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
                BufferedWriter writerXDS = new BufferedWriter(
                        new FileWriter(i++ + "_" + ((List<String>) data.get("ehr_id")).getFirst() + ".xml"));
                writerXDS.write(content);
                writerXDS.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
