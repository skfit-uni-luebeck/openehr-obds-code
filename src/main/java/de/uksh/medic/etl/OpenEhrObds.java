package de.uksh.medic.etl;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
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
import de.uksh.medic.etl.openehrmapper.Generator;
import de.uksh.medic.etl.settings.*;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlOptions;
import org.codehaus.groovy.control.CompilationFailedException;
import org.ehrbase.openehr.sdk.client.openehrclient.OpenEhrClientConfig;
import org.ehrbase.openehr.sdk.client.openehrclient.defaultrestclient.DefaultRestClient;
import org.ehrbase.openehr.sdk.generator.commons.aql.query.Query;
import org.ehrbase.openehr.sdk.response.dto.QueryResponseData;
import org.hl7.fhir.r4.model.Coding;
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
    private static UtilMethods um = new UtilMethods();
    private static IGenericClient fc;

    private OpenEhrObds() {
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException {
        InputStream settingsYaml = ClassLoader.getSystemClassLoader().getResourceAsStream("settings.yml");
        if (args.length == 1) {
            settingsYaml = new FileInputStream(args[0]);
        }

        ConfigurationLoader configLoader = new ConfigurationLoader();
        configLoader.loadConfiguration(settingsYaml, Settings.class);

        fr = new FhirResolver();
        fc = FhirContext.forR4().newRestfulGenericClient(Settings.getFhirServerUrl().toString());
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

        Settings.getMapping().values().forEach(m -> {
            initializeAttribute(m);
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
            File[] files = new File("testInput").listFiles();
            for (File f : files) {
                walkTree(mapper.readValue(f, new TypeReference<LinkedHashMap<String, Object>>() {
                }).entrySet(), 1, "", new LinkedHashMap<>());
            }
            System.exit(0);
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaUtils.getConsumerProperties());
                KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaUtils.getProducerProperties())) {
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
                        walkTree(mapper.readValue(record.value(),
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

    private static void initializeAttribute(Mapping m) {
        if (m.getTemplateId() != null) {
            OPERATIONALTEMPLATE template;
            if (openEhrClient != null) {
                Optional<OPERATIONALTEMPLATE> oTemplate = openEhrClient.templateEndpoint()
                        .findTemplate(m.getTemplateId());
                assert oTemplate.isPresent();
                template = oTemplate.get();
            } else {
                try {
                    template = OPERATIONALTEMPLATE.Factory
                            .parse(new File(new File("templates"), m.getTemplateId() + ".opt"));
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
                    Utils.formatMap((Map<String, Object>) OPENEHR_ATTRIBUTES.get(m.getTemplateId())));
            try {
                AQLS.put(m.getTemplateId(),
                        CxxMdrAttributes.getProfileAttributes(Settings.getCxxmdr(), m.getTarget(), "openehr"));
            } catch (URISyntaxException e) {
                Logger.error(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> localMap(Set<Entry<String, Object>> xmlSet, String templateId, String path) {
        Binding b = new Binding();
        GroovyShell s = new GroovyShell(b);
        b.setVariable("xmlSet", xmlSet);
        b.setVariable("path", path);
        b.setVariable("fhirClient", fc);
        b.setVariable("fhirResolver", fr);
        b.setVariable("utils", um);

        if (Settings.getDev()) {
            return javaMap(xmlSet, path, fc, fr, um);
        } else {
            try {
                File groovyFile = new File("scripts", templateId + ".groovy");
                if (groovyFile.exists()) {
                    return (Map<String, Object>) s.evaluate(groovyFile);
                }
            } catch (CompilationFailedException | IOException e) {
                throw new ProcessingException(e);
            }
        }

        return new HashMap<>();
    }

    public static Map<String, Object> javaMap(Set<Entry<String, Object>> xmlSet, String path, IGenericClient fhirClient,
            FhirResolver fhirResolver, UtilMethods utils) {
        return null;
    }

    @SuppressWarnings({ "unchecked" })
    public static void walkTree(Set<Entry<String, Object>> xmlSet, int depth, String path,
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

            Map<String, Object> mapped = localMap(xmlSet, m.getTemplateId(), path);
            if (mapped == null) {
                return;
            }
            if (Settings.getCxxmdr() != null) {
                mapped.putAll(convertMdr(xmlSet, m));
            }
            mapped.values().removeIf(Objects::isNull);
            Utils.listConv(mapped);
            if (Settings.getCxxmdr() != null) {
                mapped.entrySet().forEach(e -> FhirUtils.queryFhirTs(FHIR_ATTRIBUTES, m, e, fr));
            }
            mapped.values().removeIf(Objects::isNull);
            Map<String, Object> result = Utils.formatMap(mapped);

            boolean done = ((List<Boolean>) result.getOrDefault("done", List.of(true))).get(0);

            if (global || !done) {
                Generator.deepMergeNoReplace(resMap, result);
                theMap = resMap;
            } else {
                Generator.deepMergeNoReplace(result, resMap);
                theMap = result;
            }

            if (update && result.get("requestMethod") != null
                    && "DELETE".equals(((List<String>) result.get("requestMethod")).getFirst())) {
                Logger.info("Found DELETE entry, trying to delete composition...");
                OpenEhrUtils.deleteOpenEhrComposition(openEhrClient, AQLS, m.getTemplateId(),
                        ((List<String>) result.get("identifier")).getFirst());
                return;
            }

            if (split && done) {
                Logger.info("Building composition.");
                buildOpenEhrComposition(m.getTemplateId(), theMap);
            }
        }

        for (Entry<String, Object> entry : xmlSet) {
            String newPath = path + "/" + entry.getKey();
            int newDepth = depth + 1;

            switch (entry.getValue()) {
                case @SuppressWarnings("rawtypes") Map h -> {
                    walkTree(h.entrySet(), newDepth, newPath, theMap);
                }
                case @SuppressWarnings("rawtypes") List a -> {
                    for (Object b : a) {
                        walkTree(((Map<String, Object>) b).entrySet(), newDepth, newPath, theMap);
                    }
                }
                default -> {
                }
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
            composition = PARSERS.get(templateId).build(data,
                    (Map<String, Object>) openehrDatatypes.getOrDefault(templateId, new HashMap<>()));

        } catch (XPathExpressionException e) {
            Logger.error(e);
            throw new ProcessingException(e);
        }

        if (!data.containsKey("ehr_id")) {
            Logger.error("Found no ehr_id in the mapped object!");
            throw new ProcessingException();
        }

        if (Settings.getKafka().getUrl() == null || Settings.getKafka().getUrl().isEmpty()) {
            Logger.debug("Kafka URL is not set, writing compositon to file.");
            Logger.debug("Finished JSON-Generation. Generating String.");
            try {
                BufferedWriter writer = new BufferedWriter(
                        new FileWriter("fileOutput/" + i++ + "_"
                                + ((List<String>) data.get("ehr_id")).getFirst() + ".json"));
                ehr = JacksonUtil.getObjectMapper().writeValueAsString(composition);
                writer.write(ehr);
                writer.close();

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

        ObjectVersionId ovi = OpenEhrUtils.getVersionUid(openEhrClient, AQLS, templateId,
                ((List<String>) data.get("identifier")).getFirst());
        if (ovi != null) {
            composition.setUid(ovi);
        }
        openEhrClient.compositionEndpoint(ehrId).mergeRaw(composition);
    }

}
