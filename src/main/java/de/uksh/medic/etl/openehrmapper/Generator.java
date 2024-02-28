package de.uksh.medic.etl.openehrmapper;

import com.nedap.archie.rm.archetyped.Archetyped;
import com.nedap.archie.rm.composition.Action;
import com.nedap.archie.rm.composition.Activity;
import com.nedap.archie.rm.composition.AdminEntry;
import com.nedap.archie.rm.composition.ContentItem;
import com.nedap.archie.rm.composition.Evaluation;
import com.nedap.archie.rm.composition.Instruction;
import com.nedap.archie.rm.composition.IsmTransition;
import com.nedap.archie.rm.composition.Observation;
import com.nedap.archie.rm.composition.Section;
import com.nedap.archie.rm.datastructures.Cluster;
import com.nedap.archie.rm.datastructures.Element;
import com.nedap.archie.rm.datastructures.Event;
import com.nedap.archie.rm.datastructures.History;
import com.nedap.archie.rm.datastructures.Item;
import com.nedap.archie.rm.datastructures.ItemStructure;
import com.nedap.archie.rm.datastructures.ItemTree;
import com.nedap.archie.rm.datastructures.PointEvent;
import com.nedap.archie.rm.datatypes.CodePhrase;
import com.nedap.archie.rm.datavalues.DvBoolean;
import com.nedap.archie.rm.datavalues.DvCodedText;
import com.nedap.archie.rm.datavalues.DvIdentifier;
import com.nedap.archie.rm.datavalues.DvText;
import com.nedap.archie.rm.datavalues.DvURI;
import com.nedap.archie.rm.datavalues.quantity.DvCount;
import com.nedap.archie.rm.datavalues.quantity.DvOrdinal;
import com.nedap.archie.rm.datavalues.quantity.DvQuantity;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDate;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDateTime;
import com.nedap.archie.rm.generic.PartySelf;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.TerminologyId;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.hl7.fhir.r4.model.Coding;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class Generator {

    public static final List<Map<String, Object>> JSONMAP = new ArrayList<>();
    private static Document opt;
    private static final XPath XP = XPathFactory.newInstance().newXPath();
    private static final Map<String, String> CACHE = new HashMap<>();
    private static final Map<String, NodeList> CACHE_NODE_LIST = new LinkedHashMap<>();

    public Generator(Document opt) {
        Generator.opt = opt;
        generateCache();
    }

    private void generateCache() {
    }

    public static void processAttributeChildren(String path, String name, Object jsonmap,
            Map<String, Object> map) {
        String newPath = path + "/children";
        if (!CACHE_NODE_LIST.containsKey(newPath + "/rm_type_name")) {
            XPathExpression expr;
            try {
                expr = XP.compile(newPath + "/rm_type_name");
                CACHE_NODE_LIST.put(newPath + "/rm_type_name", (NodeList) expr.evaluate(opt, XPathConstants.NODESET));
            } catch (XPathExpressionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        NodeList children = CACHE_NODE_LIST.get(newPath + "/rm_type_name");
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) == null || children.item(i).getFirstChild() == null || map == null) {
                System.out.print("null");
                return;
            }
            String type = "gen_" + children.item(i).getFirstChild().getTextContent();
            type = type.replaceAll("[^A-Za-z_]+", "_");
            Method met;
            try {
                met = Generator.class.getMethod(type, String.class, String.class, Object.class,
                        Map.class);
                met.invoke(Generator.class, newPath + "[" + (i + 1) + "]", name, jsonmap, map);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    // Navigation Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/ehr.html#_class_descriptions_4

    @SuppressWarnings("unchecked")
    public static void gen_SECTION(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String label = getTypeLabel(path, getNodeId(path));
        String newPath = path + "/attributes";
        Section section = new Section();
        section.setArchetypeNodeId(paramName);

        section.setNameAsString(label);
        List<ContentItem> items = new ArrayList<>();
        processAttributeChildren(newPath, paramName, items, (Map<String, Object>) map.get(paramName));
        section.setItems(items);

        ((List<ContentItem>) jsonmap).add(section);
    }

    // Entry Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/ehr.html#_class_descriptions_5

    @SuppressWarnings("unchecked")
    public static void gen_ADMIN_ENTRY(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {

        String paramName = getArcheTypeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"data\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);

        AdminEntry adminEntry = new AdminEntry();
        adminEntry.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
        adminEntry.setArchetypeNodeId(paramName);
        adminEntry.setNameAsString(getLabel(getNodeId(path), paramName));
        adminEntry.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
        adminEntry.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
        adminEntry.setSubject(new PartySelf());

        if (map.containsKey(paramName)) {
            ItemTree il = new ItemTree();
            processAttributeChildren(oap, paramName, il, (Map<String, Object>) map.get(paramName));
            adminEntry.setData(il);
            if (oa) {
                ((ArrayList<ContentItem>) jsonmap).add(adminEntry);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void gen_OBSERVATION(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"data\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);

        Observation observation = new Observation();
        observation.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
        observation.setArchetypeNodeId(paramName);
        observation.setNameAsString(getLabel(getNodeId(path), paramName));
        observation.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
        observation.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
        observation.setSubject(new PartySelf());

        if (map.containsKey(paramName)) {
            History<ItemStructure> history = new History<ItemStructure>();
            processAttributeChildren(oap, paramName, history, (Map<String, Object>) map.get(paramName));
            observation.setData(history);
            if (oa) {
                ((ArrayList<ContentItem>) jsonmap).add(observation);
            }
        }
    }

    public static void gen_EVALUATION(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        // String newPath = path + "/attributes";
        Evaluation events = new Evaluation();
        events.setArchetypeNodeId(nodeId);

        events.setNameAsString(label);

        // events.setTime(new DvDateTime((String) map.get("events_time")));
        // ItemTree itemTree = new ItemTree();
        // processAttributeChildren(path, nodeId, itemTree, map);
        // events.setData(itemTree);

        // ((History<ItemStructure>) jsonmap).addEvent(events);
    }

    @SuppressWarnings("unchecked")
    public static void gen_INSTRUCTION(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oapActivities = path + "/attributes[rm_attribute_name=\"activities\"]";
        String oapProtocol = path + "/attributes[rm_attribute_name=\"protocol\"]";
        Boolean oaActivities = (Boolean) XP.evaluate(oapActivities, opt, XPathConstants.BOOLEAN);
        Boolean oaProtocol = (Boolean) XP.evaluate(oapProtocol, opt, XPathConstants.BOOLEAN);

        Instruction instruction = new Instruction();
        instruction.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
        instruction.setArchetypeNodeId(paramName);
        instruction.setNameAsString(getLabel(getNodeId(path), paramName));
        instruction.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
        instruction.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
        instruction.setSubject(new PartySelf());
        instruction.setNarrative(new DvText(""));

        if (map.containsKey(paramName)) {
            List<Activity> activities = new ArrayList<>();
            ItemTree protocol = new ItemTree();
            processAttributeChildren(oapActivities, paramName, activities, (Map<String, Object>) map.get(paramName));
            processAttributeChildren(oapProtocol, paramName, protocol, (Map<String, Object>) map.get(paramName));
            instruction.setActivities(activities);
            instruction.setProtocol(protocol);
            if (oaActivities || oaProtocol) {
                ((ArrayList<ContentItem>) jsonmap).add(instruction);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void gen_ACTIVITY(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String nodeId = getNodeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"description\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);

        Activity activity = new Activity();
        activity.setActionArchetypeId("openEHR-EHR-INSTRUCTION.service_request.v1");
        activity.setArchetypeNodeId(nodeId);
        activity.setNameAsString(getLabel(nodeId, name));

        if (map.containsKey(nodeId)) {
            ItemTree itemTree = new ItemTree();
            processAttributeChildren(oap, name, itemTree, (Map<String, Object>) map.get(nodeId));
            activity.setDescription(itemTree);
            if (oa) {
                ((ArrayList<Activity>) jsonmap).add(activity);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void gen_ACTION(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oapDescription = path + "/attributes[rm_attribute_name=\"description\"]";
        String oapProtocol = path + "/attributes[rm_attribute_name=\"protocol\"]";
        Boolean oaDescription = (Boolean) XP.evaluate(oapDescription, opt, XPathConstants.BOOLEAN);
        Boolean oaProtocol = (Boolean) XP.evaluate(oapProtocol, opt, XPathConstants.BOOLEAN);

        Action action = new Action();
        action.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
        action.setArchetypeNodeId(paramName);
        action.setNameAsString(getLabel(getNodeId(path), paramName));
        action.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
        action.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
        action.setSubject(new PartySelf());
        IsmTransition ism = new IsmTransition();
        ism.setCurrentState(
                new DvCodedText("completed", new CodePhrase(new TerminologyId("openehr"), "532", "completed")));
        action.setIsmTransition(ism);

        if (map.containsKey(paramName)) {
            action.setTime(new DvDateTime(((Map<String, List<String>>) map.get(paramName)).get("time").get(0)));
            ItemTree description = new ItemTree();
            ItemTree protocol = new ItemTree();
            processAttributeChildren(oapDescription, paramName, description, (Map<String, Object>) map.get(paramName));
            processAttributeChildren(oapProtocol, paramName, protocol, (Map<String, Object>) map.get(paramName));
            action.setDescription(description);
            action.setProtocol(protocol);

            if (oaDescription || oaProtocol) {
                ((ArrayList<ContentItem>) jsonmap).add(action);
            }
        }
    }

    // Item Structure
    // https://specifications.openehr.org/releases/RM/latest/data_structures.html#_class_descriptions_2

    // ITEM_SINGLE

    // ITEM_LIST

    // ITEM_TABLE

    public static void gen_ITEM_TREE(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        if (!CACHE.containsKey(path + "/../rm_attribute_name")) {
            XPathExpression expr = XP.compile(path + "/../rm_attribute_name");
            CACHE.put(path + "/../rm_attribute_name", (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        String attributeName = CACHE.get(path + "/../rm_attribute_name");
        String nodeId = getNodeId(path);
        String newPath = path + "/attributes";
        if ("".equals(attributeName)) {
            return;
        }

        ItemTree itemTree = (ItemTree) jsonmap;
        itemTree.setArchetypeNodeId(nodeId);
        itemTree.setNameAsString("data"); // fix name
        ArrayList<Item> items = new ArrayList<Item>();
        itemTree.setItems(items);
        processAttributeChildren(newPath, name, items, map);
    }

    // Representation Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_structures.html#_class_descriptions_3

    @SuppressWarnings("unchecked")
    public static void gen_CLUSTER(String path, String name, Object jsonmap,
            Map<String, Object> map)
            throws Exception {
        if (!CACHE.containsKey(path + "/archetype_id")) {
            XPathExpression expr = XP.compile(path + "/archetype_id");
            CACHE.put(path + "/archetype_id",
                    ((String) expr.evaluate(opt, XPathConstants.STRING)).trim());
        }
        String cAID = CACHE.get(path + "/archetype_id");
        String paramName = !"".equals(cAID) ? cAID : name;
        String nodeId = getNodeId(path);
        Boolean isSlot = ((String) XP.evaluate(path + "/@type", opt, XPathConstants.STRING)).equals("C_ARCHETYPE_ROOT");
        String aNodeId = isSlot ? paramName : nodeId;
        String newPath = path + "/attributes";
        String code = !"".equals(paramName) && !name.equals(paramName) ? paramName : nodeId;
        if (!map.containsKey(code)) {
            return;
        }
        String label = getLabel(nodeId, paramName);
        Cluster cluster = new Cluster();
        cluster.setArchetypeNodeId(aNodeId);
        cluster.setNameAsString(label);
        ArrayList<Item> items = new ArrayList<Item>();
        processAttributeChildren(newPath, paramName, items, (Map<String, Object>) map.get(code));
        cluster.setItems(items);
        ((ArrayList<Object>) jsonmap).add(cluster);
    }

    @SuppressWarnings("unchecked")
    public static void gen_ELEMENT(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String nodeId = getNodeId(path);
        String newPath = path + "/attributes[rm_attribute_name = \"value\"]";
        if (!map.containsKey(nodeId)) {
            return;
        }
        String label = getLabel(nodeId, name);

        ((List<Object>) map.get(nodeId)).forEach(e -> {
            Element el = new Element();
            el.setArchetypeNodeId(nodeId);
            el.setNameAsString(label);
            Map<String, Object> mo = new HashMap<>();
            mo.put(nodeId, e);
            mo.put("name", name);
            if (e instanceof Map || e instanceof List) {
                return;
            }
            processAttributeChildren(newPath, nodeId, el, mo);
            ((ArrayList<Element>) jsonmap).add(el);

        });
    }

    // HISTORY Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_structures.html#_class_descriptions_4

    @SuppressWarnings("unchecked")
    public static void gen_HISTORY(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        String newPath = path + "/attributes";
        History<ItemStructure> history = (History<ItemStructure>) jsonmap;
        history.setArchetypeNodeId(nodeId);

        history.setNameAsString(label);

        history.setOrigin(new DvDateTime((String) map.get("events_time")));

        processAttributeChildren(newPath, nodeId, history, map);
    }

    @SuppressWarnings("unchecked")
    public static void gen_EVENT(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        String newPath = path + "/attributes";
        Event<ItemStructure> events = new PointEvent<ItemStructure>();
        events.setArchetypeNodeId(nodeId);

        events.setNameAsString(label);

        events.setTime(new DvDateTime((String) map.get("events_time")));
        ItemTree itemTree = new ItemTree();
        processAttributeChildren(newPath, nodeId, itemTree, map);
        events.setData(itemTree);

        ((History<ItemStructure>) jsonmap).addEvent(events);
    }

    // Datatypes
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_data_types_information_model

    // Basic Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions

    public static void gen_DV_BOOLEAN(String path, String name, Object jsonmap,
            Map<String, Boolean> map) {
        ((Element) jsonmap).setValue(new DvBoolean(map.get(name)));
    }

    // DV_STATE

    public static void gen_DV_IDENTIFIER(String path, String name, Object jsonmap,
            Map<String, Object> map) throws Exception {
        DvIdentifier id = new DvIdentifier();
        id.setId(String.valueOf(map.get(name)));
        ((Element) jsonmap).setValue(id);
    }

    // Text Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_2

    public static void gen_DV_TEXT(String path, String name, Object jsonmap,
            Map<String, String> map)
            throws Exception {
        if (!map.containsKey(name)) {
            return;
        }
        ((Element) jsonmap).setValue(new DvText(map.get(name)));
    }

    // TERM_MAPPING

    // CODE_PHRASE

    public static void gen_DV_CODED_TEXT(String path, String name, Object jsonmap,
            Map<String, Object> map) throws Exception {

        DvCodedText ct = new DvCodedText();
        switch (map.get(name)) {
            case Coding coding -> {
                ct.setDefiningCode(new CodePhrase(
                        new TerminologyId("terminology://fhir.hl7.org//ValueSet/$expand?url=" + coding.getSystem(),
                                coding.getVersion()),
                        coding.getCode(), coding.getDisplay()));
                ct.setValue(coding.getDisplay());
                ((Element) jsonmap).setValue(ct);
            }
            case String s -> {
                String display = getLocalTerminologyTerm((String) map.get("name"), s);
                switch (display) {
                    case "" -> {
                        String local = getLocalTerm(path, s);
                        ct.setDefiningCode(new CodePhrase(
                                new TerminologyId("local"),
                                local, s));
                        ct.setValue(s);
                    }
                    default -> {
                        ct.setDefiningCode(new CodePhrase(
                                new TerminologyId("local_terms"),
                                s, display));
                        ct.setValue(display);
                    }
                }

                ((Element) jsonmap).setValue(ct);
            }
            default -> {
            }
        }

    }

    // DV_PARAGRAPH

    // Quantity Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_3

    // DV_INTERVAL

    // REFERENCE_RANGE

    public static void gen_DV_ORDINAL(String path, String name, Object jsonmap,
            Map<String, String> map) throws Exception {
        DvOrdinal dvo = new DvOrdinal();
        Long value = Long.valueOf(map.get(name));
        dvo.setValue(value);
        String ordinal = getOrdinal(path, map.get(name));
        String display = getLabel(ordinal, map.get("name"));
        DvCodedText ct = new DvCodedText(display, new CodePhrase(new TerminologyId("local_terms"), ordinal, display));
        dvo.setSymbol(ct);
        ((Element) jsonmap).setValue(dvo);
    }
    // DV_SCALE

    // DV_QUANTIFIED

    // DV_AMOUNT

    public static void gen_DV_QUANTITY(String path, String name, Object jsonmap,
            Map<String, String> map) {
        ((Element) jsonmap).setValue(new DvQuantity("1", Double.valueOf(map.get(name)), 1L));
    }

    public static void gen_DV_COUNT(String path, String name, Object jsonmap,
            Map<String, Long> map) {
        ((Element) jsonmap).setValue(new DvCount(map.get(name)));
    }

    // DV_PROPORTION

    // PROPORTION_KIND

    // DV_ABSOLUTE_QUANTITY

    // DateTime Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_4

    public static void gen_DV_DATE(String path, String name, Object jsonmap,
            Map<String, String> map) {
        ((Element) jsonmap).setValue(new DvDate(map.get(name)));
    }

    // DV_TIME

    public static void gen_DV_DATE_TIME(String path, String name, Object jsonmap,
            Map<String, String> map) {
        ((Element) jsonmap).setValue(new DvDateTime(map.get(name)));
    }

    // DV_DURATION

    // Time Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_5

    // DV_PERIODIC_TIME_SPECIFICATION

    // DV_GENERAL_TIME_SPECIFICATION

    // Encapsulated Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_6

    // DV_MULTIMEDIA

    // DV_PARSABLE

    // URI Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_7

    public static void gen_DV_URI(String path, String name, Object jsonmap,
            Map<String, Object> map)
            throws Exception {
        ((Element) jsonmap).setValue(new DvURI(String.valueOf(map.get(name))));
    }

    // DV_EHR_URI

    // XPath Query functions

    private static String getOrdinal(String path, String code) throws Exception {
        String newPath = path + "/list[value/text()=\"" + code + "\"]/symbol/defining_code/code_string/text()";
        if (!CACHE.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            CACHE.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return CACHE.get(newPath);
    }

    private static String getLabel(String code, String archetype) throws Exception {
        String path = "//archetype_id[value=\"" + archetype + "\"]/../term_definitions[@code=\"" + code
                + "\"]/items[@id=\"text\"]/text()";
        if (!CACHE.containsKey(path)) {
            XPathExpression expr = XP.compile(path);
            CACHE.put(path, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return CACHE.get(path);
    }

    private static String getTypeLabel(String path, String code) throws Exception {
        String newPath = path + "/term_definitions[@code=\"" + code + "\"]/items[@id=\"text\"]";
        if (!CACHE.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            CACHE.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return CACHE.get(newPath);
    }

    private static String getLocalTerminologyTerm(String archetype, String code) throws Exception {
        String newPath = "//archetype_id[value/text()=\"" + archetype + "\"]/../term_definitions[@code=\"local_terms::"
                + code + "\"]/items/text()";
        if (!CACHE.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            CACHE.put(newPath, ((String) expr.evaluate(opt, XPathConstants.STRING)).replaceAll("^* (?m) ", "")
                    .replaceAll("\\n", " "));
        }
        return CACHE.get(newPath);
    }

    private static String getLocalTerm(String path, String code) throws Exception {
        String newPath = "//term_definitions[items/@id=\"text\"][items/text()=\"" + code + "\"]/@code";
        if (!CACHE.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            CACHE.put(newPath, ((String) expr.evaluate(opt, XPathConstants.STRING)).replaceAll("^* (?m) ", "")
                    .replaceAll("\\n", " "));
        }
        return CACHE.get(newPath);
    }

    private static String getNodeId(String path) throws Exception {
        String newPath = path + "/node_id";
        if (!CACHE.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            CACHE.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return CACHE.get(newPath);
    }

    private static String getArcheTypeId(String path) throws Exception {
        String newPath = path + "/archetype_id/value";
        if (!CACHE.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            CACHE.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return CACHE.get(newPath);
    }

    public static Map<String, Object> getDefaultValues() {
        Map<String, Object> defaults = new HashMap<>();
        Map<String, Object> current = defaults;
        String path = "//constraints/attributes[children/default_value]";
        try {
            XPathExpression expr = XP.compile(path);
            NodeList nl = (NodeList) expr.evaluate(opt, XPathConstants.NODESET);
            for (int i = 1; i <= nl.getLength(); i++) {
                XPathExpression exprC = XP.compile(path + "[" + i + "]/children/default_value/value/text()");
                XPathExpression exprP = XP.compile(path + "[" + i + "]/differential_path/text()");
                String value = (String) exprC.evaluate(opt, XPathConstants.STRING);
                String differentialPath = ((String) exprP.evaluate(opt, XPathConstants.STRING)).trim();
                List<String> l = List.of(differentialPath.split("/"));
                String last = l.getLast().split("(\\[|\\])")[1].replaceAll(",.*", "");
                for (int j = 1; j < l.size() - 1; j++) {
                    String s = l.get(j);
                    if (s.contains("description")) {
                        continue;
                    }
                    Map<String, Object> n = new HashMap<>();
                    String s2 = s.split("(\\[|\\])")[1].replaceAll(",.*", "");
                    current.put(s2, n);
                    current = n;
                }
                List<String> ls = new ArrayList<>();
                ls.add(value);
                current.put(last, ls);
            }
        } catch (XPathExpressionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return defaults;
    }

    public static Map<String, Object> applyDefaults(Map<String, Object> map) {
        Map<String, Object> defaults = getDefaultValues();
        deepMerge(defaults, map);
        return defaults;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void deepMerge(Map<String, Object> map1, Map<String, Object> map2) {
        for (String key : map2.keySet()) {
            Object value2 = map2.get(key);
            if (map1.containsKey(key)) {
                Object value1 = map1.get(key);
                if (value1 instanceof Map && value2 instanceof Map) {
                    deepMerge((Map<String, Object>) value1, (Map<String, Object>) value2);
                } else if (value1 instanceof List && value2 instanceof List) {
                    map1.put(key, merge((List) value1, (List) value2));
                } else {
                    map1.put(key, value2);
                }
            } else {
                map1.put(key, value2);
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List merge(List list1, List list2) {
        list2.removeAll(list1);
        list1.addAll(list2);
        return list1;
    }

}
