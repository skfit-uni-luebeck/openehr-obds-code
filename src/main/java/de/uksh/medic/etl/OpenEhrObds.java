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
import de.uksh.medic.etl.jobs.mdr.centraxx.*;
import de.uksh.medic.etl.model.MappingAttributes;
import de.uksh.medic.etl.model.mdr.centraxx.CxxItemSet;
import de.uksh.medic.etl.model.mdr.centraxx.RelationConvert;
import de.uksh.medic.etl.openehrmapper.EHRParser;
import de.uksh.medic.etl.settings.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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

    private static final Map<String, Map<String, MappingAttributes>> FHIR_ATTRIBUTES = new HashMap<>();
    private static final Map<String, Object> OPENEHR_ATTRIBUTES = new HashMap<>();
    private static final Map<String, MappingAttributes> AQLS = new HashMap<>();
    private static final Map<String, EHRParser> PARSERS = new HashMap<>();
    private static Integer i = 0;
    private static DefaultRestClient openEhrClient;
    private static Map<String, Object> openehrDatatypes = new HashMap<>();
    private static FhirResolver fr;

    private OpenEhrObds() {
    }

    public static void main(String[] args) throws IOException {
        InputStream settingsYaml = ClassLoader.getSystemClassLoader().getResourceAsStream("settings.yml");
        if (args.length == 1) {
            settingsYaml = new FileInputStream(args[0]);
        }

        ConfigurationLoader configLoader = new ConfigurationLoader();
        configLoader.loadConfiguration(settingsYaml, Settings.class);

        fr = new FhirResolver();
        CxxMdrSettings mdrSettings = Settings.getCxxmdr();
        if (mdrSettings != null) {
            CxxMdrLogin.login(mdrSettings);
        }

        URI ehrBaseUrl = Settings.getOpenEhrUrl();
        if (ehrBaseUrl != null && Settings.getOpenEhrUser() != null && Settings.getOpenEhrPassword() != null) {
            String credentials = ehrBaseUrl.toString();
            try {
                ehrBaseUrl = new URI(credentials.replace("://",
                        "://" + URLEncoder.encode(Settings.getOpenEhrUser(), StandardCharsets.UTF_8) + ":"
                                + URLEncoder.encode(Settings.getOpenEhrPassword(), StandardCharsets.UTF_8)
                                + "@"));
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
            if (Settings.getCxxmdr() != null) {
                CxxItemSet is = CxxMdrItemSet.get(Settings.getCxxmdr(), m.getTarget());
                is.getItems().forEach(it -> {
                    try {
                        if (!FHIR_ATTRIBUTES.containsKey(m.getTarget())) {
                            FHIR_ATTRIBUTES.put(m.getTarget(), new HashMap<>());
                        }
                        FHIR_ATTRIBUTES.getOrDefault(m.getTarget(), new HashMap<>()).put(it.getId(),
                                CxxMdrAttributes.getAttributes(Settings.getCxxmdr(), m.getTarget(), "fhir", it.getId()));
    
                        if (!OPENEHR_ATTRIBUTES.containsKey(m.getTemplateId())) {
                            OPENEHR_ATTRIBUTES.put(m.getTemplateId(), new HashMap<>());
                        }
                        ((Map<String, Object>) OPENEHR_ATTRIBUTES.getOrDefault(m.getTemplateId(), new HashMap<>())).put(
                                it.getId(),
                                CxxMdrAttributes.getAttributes(Settings.getCxxmdr(), m.getTarget(), "openehr", it.getId()));
                    } catch (URISyntaxException e) {
                        Logger.error(e);
                    }
                });
                openehrDatatypes.put(m.getTemplateId(),
                        formatMap((Map<String, Object>) OPENEHR_ATTRIBUTES.get(m.getTemplateId())));
                try {
                    AQLS.put(m.getTemplateId(),
                            CxxMdrAttributes.getProfileAttributes(Settings.getCxxmdr(), m.getTarget(), "openehr"));
                } catch (URISyntaxException e) {
                    Logger.error(e);
                }
            }

        });

        ObjectMapper mapper;

        if ("xml".equals(Settings.getMode())) {
            mapper = new XmlMapper();
        } else {
            mapper = new JsonMapper();
        }

        Logger.info("OpenEhrObds started!");

        if (Settings.getKafka().getUrl() == null || Settings.getKafka().getUrl().isEmpty()) {
            Logger.debug("Kafka URL not set, loading local file");
            File f = new File("PUTbundleHEB.json");
            // File f = new File("op.xml");

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
                Logger.debug("Polling Kafka topic");
                ConsumerRecords<String, String> records = consumer.poll(
                        Duration.ofMillis(Settings.getKafka().getPollDuration()));

                for (ConsumerRecord<String, String> record : records) {
                    Logger.debug("Processing record.");
                    try {
                        walkXmlTree(mapper.readValue(record.value(),
                                new TypeReference<LinkedHashMap<String, Object>>() {
                                }).entrySet(), 1, "",
                                new LinkedHashMap<>());
                    } catch (ProcessingException e) {
                        Logger.error("ProcessingException occured, writing to error topic!");
                        producer.send(new ProducerRecord<>(Settings.getKafka().getErrorTopic(), record.value()));
                        producer.flush();
                    }
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            Logger.debug("Caught wake up exception.");
        }
    }

    private static Properties getConsumerProperties() {
        KafkaSettings settings = Settings.getKafka();
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, settings.getClientID());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, settings.getGroup());
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getUrl());
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, settings.getOffset());
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.getPollRecords());
        // 2min max intervall to process getPollRecords() bundle
        consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000");
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

    @SuppressWarnings({ "unchecked" })
    public static void walkXmlTree(Set<Entry<String, Object>> xmlSet, int depth, String path,
            Map<String, Object> resMap) {
        if (depth > Settings.getDepthLimit()) {
            return;
        }

        boolean split = Settings.getMapping().containsKey(path) && Settings.getMapping().get(path).getSplit();
        boolean global = Settings.getMapping().containsKey(path) && Settings.getMapping().get(path).getGlobal();
        boolean update = Settings.getMapping().containsKey(path) && Settings.getMapping().get(path).isUpdate();

        Map<String, Object> theMap = resMap;

        if (Settings.getMapping().containsKey(path) && Settings.getMapping().get(path).getSource() != null) {
            Mapping m = Settings.getMapping().get(path);

            Map<String, Object> mapped = new HashMap<>();
            // ToDo: Add Switch!
            if (Settings.getCxxmdr() != null) {
                mapped.putAll(convertMdr(xmlSet, m));
            }
            assert mapped != null;
            mapped.values().removeIf(Objects::isNull);
            listConv(mapped);
            if (Settings.getCxxmdr() != null) {
                mapped.entrySet().forEach(e -> queryFhirTs(m, e));
            }
            mapped.values().removeIf(Objects::isNull);
            Map<String, Object> result = formatMap(mapped);

            if (global) {
                resMap.putAll(result);
                theMap = resMap;
            } else {
                result.putAll(resMap);
                theMap = result;
            }

            if (update && result.get("requestMethod") != null
                    && "DELETE".equals(((List<String>) result.get("requestMethod")).getFirst())) {
                Logger.info("Found DELETE entry, trying to delete composition...");
                deleteOpenEhrComposition(m.getTemplateId(), ((List<String>) result.get("cxxId")).getFirst());
                return;
            }

            if (split) {
                Logger.info("Building composition.");
                buildOpenEhrComposition(m.getTemplateId(), theMap);
            }
        }

        for (Entry<String, Object> entry : xmlSet) {
            String newPath = path + "/" + entry.getKey();
            int newDepth = depth + 1;

            switch (entry.getValue()) {
                case @SuppressWarnings("rawtypes") Map h -> {
                    walkXmlTree(h.entrySet(), newDepth, newPath, theMap);
                }
                case @SuppressWarnings("rawtypes") List a -> {
                    for (Object b : a) {
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
    private static void queryFhirTs(Mapping m, Entry<String, Object> e) {
        if (e.getValue() == null) {
            return;
        }
        MappingAttributes fa = FHIR_ATTRIBUTES.get(m.getTarget()).get(e.getKey());
        List<Object> listed = new ArrayList<>();
        for (Object o : (List) e.getValue()) {
            if (fa != null && fa.getTarget() != null && "http://unitsofmeasure.org".equals(fa.getTarget().toString())
                    && fa.getConceptMap() != null) {
                switch (o) {
                    case String c -> listed.add(o);
                    case Map map when map.containsKey("magnitude") && map.containsKey("unit") -> {
                        String[] newMagnitude = CxxMdrUnitConvert.convert(Settings.getCxxmdr(), map, fa);
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

    @SuppressWarnings({ "unchecked" })
    private static void splitMap(Object value, List<String> key, Map<String, Object> out) {
        if (key.size() > 1) {
            String k = key.removeFirst();
            if (!(out.get(k) instanceof List)) {
                Map<String, Object> m = (Map<String, Object>) out.getOrDefault(k,
                        new LinkedHashMap<>());
                out.put(k, m);
                splitMap(value, key, m);

            } else {
                List<Map<String, Object>> l = (List<Map<String, Object>>) out.get(k);
                Map<String, Object> m = l.get(0);
                out.put(k, m);
                splitMap(value, key, m);
            }
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
        conv.setValues(xmlSet.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
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

        try {
            // Write JSON to file
            composition = PARSERS.get(templateId).build(data, (Map<String, Object>) openehrDatatypes.getOrDefault(templateId, new HashMap<>()));

            Logger.debug("Finished JSON-Generation. Generating String.");
            ehr = JacksonUtil.getObjectMapper().writeValueAsString(composition);

        } catch (XPathExpressionException | JsonProcessingException e) {
            Logger.error(e);
            throw new ProcessingException(e);
        }

        if (!data.containsKey("ehr_id")) {
            Logger.error("Found no ehr_id in the mapped object!");
            throw new ProcessingException();
        }

        if (Settings.getKafka().getUrl() == null || Settings.getKafka().getUrl().isEmpty()) {
            Logger.debug("Kafka URL is not set, writing compositon to file.");
            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter("fileOutput/" + i++ + "_"
                            + ((List<String>) data.get("ehr_id")).getFirst() + ".json"))) {
                writer.write(ehr);
            } catch (IOException e) {
                throw new ProcessingException(e);
            }
        }

        String ehrIdString = ((List<String>) data.get("ehr_id")).getFirst();
        QueryResponseData ehrIds = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                "SELECT e/ehr_id/value as EHR_ID"
                        + " FROM EHR e WHERE e/ehr_status/subject/external_ref/id/value = '"
                        + ehrIdString + "'"));
        UUID ehrId;
        if (ehrIds.getRows() == null) {
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

        ObjectVersionId ovi = getVersionUid(templateId, ((List<String>) data.get("cxxId")).getFirst());
        if (ovi != null) {
            composition.setUid(ovi);
        }
        openEhrClient.compositionEndpoint(ehrId).mergeRaw(composition);
    }

    private static void deleteOpenEhrComposition(String templateId, String itemId) throws ProcessingException {
        if (AQLS.get(templateId).getDeleteAql() == null) {
            Logger.warn("Cannot delete composition because deleteAql query not set.");
            return;
        }
        QueryResponseData ehrIds = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                String.format(AQLS.get(templateId).getDeleteAql(), templateId,
                        Settings.getSystemId(), itemId)));
        if (ehrIds.getRows() == null) {
            Logger.info("Nothing to delete for templateId {}, originalId {} from system: {}",
                    templateId, itemId, Settings.getSystemId());
            return;
        }

        if (ehrIds.getRows().size() > 1) {
            Logger.error("Found more than one composition to delete for ID: {} from system: {}!"
                    + " This should not happen!", itemId, Settings.getSystemId());
            throw new ProcessingException();
        }

        UUID ehrId = UUID.fromString((String) ehrIds.getRows().getFirst().getFirst());
        ObjectVersionId versionId = new ObjectVersionId((String) ehrIds.getRows().getFirst().getLast());
        Logger.info("Deleting composition {} from ehr {}", versionId, ehrId);
        openEhrClient.compositionEndpoint(ehrId).delete(versionId);
        return;
    }

    private static ObjectVersionId getVersionUid(String templateId, String itemId) throws ProcessingException {
        if (!AQLS.containsKey(templateId) || AQLS.get(templateId).getUpdateAql() == null) {
            Logger.warn("Cannot update composition because updateAql query not set.");
            return null;
        }
        QueryResponseData ehrIds = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                String.format(AQLS.get(templateId).getUpdateAql(), templateId,
                        Settings.getSystemId(), itemId)));
        if (ehrIds.getRows() == null) {
            Logger.info("No composition found for templateId {}, originalId {} from system: {}",
                    templateId, itemId, Settings.getSystemId());
            return null;
        }

        if (ehrIds.getRows().size() > 1) {
            Logger.error("Found more than one composition for ID: {} from system: {}!"
                    + " This should not happen!", itemId, Settings.getSystemId());
            throw new ProcessingException();
        }

        return new ObjectVersionId((String) ehrIds.getRows().getFirst().getFirst());
    }

}
