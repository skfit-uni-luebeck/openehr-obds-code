package de.uksh.medic.etl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.nedap.archie.json.JacksonUtil;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.datavalues.DvText;
import com.nedap.archie.rm.ehr.EhrStatus;
import com.nedap.archie.rm.generic.PartySelf;
import com.nedap.archie.rm.support.identification.HierObjectId;
import com.nedap.archie.rm.support.identification.ObjectVersionId;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPathExpressionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlOptions;
import org.ehrbase.openehr.sdk.client.openehrclient.OpenEhrClientConfig;
import org.ehrbase.openehr.sdk.client.openehrclient.defaultrestclient.DefaultRestClient;
import org.ehrbase.openehr.sdk.generator.commons.aql.query.Query;
import org.ehrbase.openehr.sdk.response.dto.QueryResponseData;
import org.openehr.schemas.v1.OPERATIONALTEMPLATE;
import org.tinylog.Logger;

public final class OpenEhrObds {

    private static final int POLL_DURATION = 1000;
    private static final Map<String, Map<String, MappingAttributes>> FHIR_ATTRIBUTES = new HashMap<>();
    private static final Map<String, EHRParser> PARSERS = new HashMap<>();
    private static Integer i = 0;
    private static DefaultRestClient openEhrClient;

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

        openEhrClient = new DefaultRestClient(new OpenEhrClientConfig(ehrBaseUrl));

        URI finalEhrBaseUrl = ehrBaseUrl;
        Settings.getMapping().values().forEach(m -> {
            if (m.getTemplateId() != null) {
                OPERATIONALTEMPLATE template;
                if (finalEhrBaseUrl != null) {
                    Optional<OPERATIONALTEMPLATE> oTemplate = openEhrClient.templateEndpoint()
                            .findTemplate(m.getTemplateId());
                    assert oTemplate.isPresent();
                    template = oTemplate.get();
                } else {
                    try {
                        template = OPERATIONALTEMPLATE.Factory.parse(new File(m.getTemplateId() + ".opt"));
                    } catch (XmlException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                XmlOptions opts = new XmlOptions();
                opts.setSaveSyntheticDocumentElement(new QName("http://schemas.openehr.org/v1", "template"));
                PARSERS.put(m.getTemplateId(), new EHRParser(template.xmlText(opts)));
            }

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
                    Logger.error(e);
                }
            });

        });

        ObjectMapper mapper;

        if ("xml".equals(Settings.getMode())) {
            mapper = new XmlMapper();
        } else {
            mapper = new JsonMapper();
        }

        Logger.info("OpenEhrObds started!");

        if (Settings.getKafka().getUrl().isEmpty()) {
            Logger.debug("Kafka URL not set, loading local file");
            File f = new File("bundle.json");
            //File f = new File("op.xml");

            walkXmlTree(mapper.readValue(f, new TypeReference<LinkedHashMap<String, Object>>() {
            }).entrySet(), 1, "", new LinkedHashMap<>());
            System.exit(0);
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());
             KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties())) {
            final Thread mainThread = Thread.currentThread();

            // adding the shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                Logger.info("Shutdown detected, calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    Logger.error(e);
                }
            }));

            consumer.subscribe(Collections.singleton(Settings.getKafka().getReadTopic()));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_DURATION));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        walkXmlTree(mapper.readValue(record.value(),
                                new TypeReference<LinkedHashMap<String, Object>>() {}).entrySet(), 1, "",
                                    new LinkedHashMap<>());
                    } catch (ProcessingException e) {
                        producer.send(new ProducerRecord<>(Settings.getKafka().getErrorTopic(), record.value()));
                        producer.flush();
                    }
                }
            }
        } catch (WakeupException e) {
            Logger.debug("Caught wake up exception.");
        }
    }

    private static Properties getConsumerProperties() {
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, Settings.getKafka().getClientID());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Settings.getKafka().getGroup());
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Settings.getKafka().getUrl());
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Settings.getKafka().getOffset());
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return consumerConfig;
    }

    private static Properties getProducerProperties() {
        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, Settings.getKafka().getClientID());
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Settings.getKafka().getUrl());
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerConfig;
    }

    @SuppressWarnings({"unchecked"})
    public static void walkXmlTree(Set<Entry<String, Object>> xmlSet, int depth, String path,
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

        for (Entry<String, Object> entry : xmlSet) {
            String newPath = path + "/" + entry.getKey();
            int newDepth = depth + 1;

            switch (entry.getValue()) {
                case @SuppressWarnings("rawtypes")Map h -> {
                    walkXmlTree(h.entrySet(), newDepth, newPath, theMap);
                }
                case @SuppressWarnings("rawtypes")List a -> {
                    for (Object b : a) {
                        Logger.debug(entry.getKey());
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void queryFhirTs(Mapping m, Entry<String, Object> e) {
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
                        case String ignored -> fa.getVersion();
                        case Map map -> ((Map<String, String>) map).get("version");
                        default -> null;
                    };
                    listed.add(FhirResolver.lookUp(fa.getSystem(), version, code));
                } else if (fa.getConceptMap() != null) {
                    listed.add(FhirResolver.conceptMap(fa.getConceptMap(), fa.getSystem(), fa.getSource(),
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

    @SuppressWarnings({"unchecked"})
    private static void splitMap(Object value, List<String> key, Map<String, Object> out) {
        if (key.size() > 1) {
            String k = key.removeFirst();
            Map<String, Object> m = (Map<String, Object>) out.getOrDefault(k,
                    new LinkedHashMap<>());
            out.put(k, m);
            splitMap(value, key, m);
        } else if (key.size() == 1 && !out.containsKey(key.getFirst())) {
            out.put(key.removeFirst(), value);
        } else if (key.size() == 1 && out.containsKey(key.getFirst())) {
            if (value instanceof List && out.get(key.getFirst()) instanceof List) {
                ((List<Object>) out.get(key.getFirst())).addAll((List<Object>) value);
            }
            if (value instanceof List && out.get(key.getFirst()) instanceof Map) {
                ((List<Map<String, Object>>) value)
                        .forEach(m -> m.putAll((Map<String, Object>) out.get(key.getFirst())));
                out.put(key.getFirst(), value);
            }
            if (value instanceof Map && out.get(key.getFirst()) instanceof Map) {
                ((Map<String, Object>) out.get(key.getFirst())).putAll((Map<String, Object>) value);
            }
            if (value instanceof Map && out.get(key.getFirst()) instanceof List) {
                ((List<Map<String, Object>>) out.get(key.getFirst()))
                        .forEach(l -> l.putAll((Map<String, Object>) value));
                out.put(key.getFirst(), value);
            }
        }
    }

    private static Map<String, Object> convertMdr(Set<Entry<String, Object>> xmlSet, Mapping m)
            throws ProcessingException {
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
            Logger.error(e);
            throw new ProcessingException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void buildOpenEhrComposition(String templateId, Map<String, Object> data)
            throws ProcessingException {
        String ehr;
        Composition composition;

        if (data.get("requestMethod") != null && "DELETE".equals(((List<String>) data.get("requestMethod")).getFirst())
                && "KDS_Biobank".equals(templateId)) {
            deleteOpenEhrComposition(((List<String>) data.get("cxxId")).getFirst());
            return;
        } else {
            data.remove("requestMethod");
        }

        try {
            // Write JSON to file
            composition = PARSERS.get(templateId).build(data);

            Logger.debug("Finished JSON-Generation. Generating String.");
            ehr = JacksonUtil.getObjectMapper().writeValueAsString(composition);

        } catch (XPathExpressionException | JsonProcessingException e) {
            Logger.error(e);
            throw new ProcessingException(e);
        }

        if (Settings.getKafka().getUrl().isEmpty()) {
            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter("fileOutput/" + i++ + "_"
                            + ((List<String>) data.get("ehr_id")).getFirst() + ".json"))) {
                writer.write(ehr);
            } catch (IOException e) {
                throw new ProcessingException(e);
            }
        }

        switch (Settings.getTarget()) {
            case "raw":
                String ehrIdString = ((List<String>) data.get("ehr_id")).getFirst();
                QueryResponseData ehrIds = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                    "SELECT e/ehr_id/value as EHR_ID FROM EHR e WHERE e/ehr_status/subject/external_ref/id/value = '"
                            + ehrIdString + "'"));
                UUID ehrId;
                if (ehrIds.getRows().isEmpty()) {
                    EhrStatus es = new EhrStatus();
                    es.setArchetypeNodeId("openEHR-EHR-EHR_STATUS.generic.v1");
                    es.setName(new DvText("EHR status"));
                    es.setQueryable(true);
                    es.setModifiable(true);
                    es.setSubject(new PartySelf(new PartyRef(
                            new HierObjectId(ehrIdString), "DEMOGRAPHIC", "PERSON")));
                    ehrId = openEhrClient.ehrEndpoint().createEhr(es);
                } else if (ehrIds.getRows().size() == 1) {
                    ehrId = UUID.fromString((String) ehrIds.getRows().getFirst().getFirst());
                } else {
                    Logger.error("Found more than one EHR for ehr_id {}!", ehrIdString);
                    throw new ProcessingException();
                }
                openEhrClient.compositionEndpoint(ehrId).mergeRaw(composition);
                break;
            case "xds":
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
                    throw new ProcessingException(e);
                }
                break;
            default:
        }
    }

    private static void deleteOpenEhrComposition(String sampleId) throws ProcessingException {
        switch (Settings.getTarget()) {
            case "raw":
                QueryResponseData ehrIds = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                    "SELECT e/ehr_id/value AS ehr_id, c/uid/value AS uid_based_id "
                            + "FROM EHR e "
                            + "CONTAINS COMPOSITION c#KDS_Biobank "
                            + "WHERE c/system_id = '" + Settings.getSystemId() + "'"
                            + "AND c/id = '" + sampleId + "'"));
                if (ehrIds.getRows().isEmpty()) {
                    Logger.info("Nothing to delete for ID: {} from system: {}", sampleId, Settings.getSystemId());
                    return;
                }

                if (ehrIds.getRows().size() > 1) {
                    Logger.error("Found more than one composition to delete for ID: {} from system: {}!"
                                   + " This should not happen!", sampleId, Settings.getSystemId());
                    throw new ProcessingException();
                }

                UUID ehrId = UUID.fromString((String) ehrIds.getRows().getFirst().getFirst());
                ObjectVersionId versionId = new ObjectVersionId((String) ehrIds.getRows().getFirst().getLast());
                Logger.info("Deleting composition {} from ehr {}", versionId, ehrId);
                openEhrClient.compositionEndpoint(ehrId).delete(versionId);
                return;
            case "xds":
                return;
            default:
        }
    }
}
