package de.uksh.medic.etl.openehrmapper;

import com.nedap.archie.rm.archetyped.Archetyped;
import com.nedap.archie.rm.composition.Action;
import com.nedap.archie.rm.composition.Activity;
import com.nedap.archie.rm.composition.AdminEntry;
import com.nedap.archie.rm.composition.ContentItem;
import com.nedap.archie.rm.composition.Evaluation;
import com.nedap.archie.rm.composition.Instruction;
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
import com.nedap.archie.rm.datavalues.DataValue;
import com.nedap.archie.rm.datavalues.DvBoolean;
import com.nedap.archie.rm.datavalues.DvCodedText;
import com.nedap.archie.rm.datavalues.DvIdentifier;
import com.nedap.archie.rm.datavalues.DvText;
import com.nedap.archie.rm.datavalues.DvURI;
import com.nedap.archie.rm.datavalues.quantity.DvCount;
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
        if (!CACHE_NODE_LIST.containsKey(path + "/rm_type_name")) {
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
            if (children.item(i) == null) {
                System.out.print("null");
            }
            if (children.item(i).getFirstChild() == null) {
                continue;
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

    public static void gen_SECTION(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        // String newPath = path + "/attributes";
        Section section = new Section();
        section.setArchetypeNodeId(nodeId);

        section.setNameAsString(label);

        // events.setTime(new DvDateTime((String) map.get("events_time")));
        // ItemTree itemTree = new ItemTree();
        // processAttributeChildren(path, nodeId, itemTree, map);
        // section.setItems(itemTree);

        // ((History<ItemStructure>) jsonmap).addEvent(events);
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
        String oap = path + "/attributes[rm_attribute_name=\"activities\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);

        Instruction instruction = new Instruction();
        instruction.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
        instruction.setArchetypeNodeId(paramName);
        instruction.setNameAsString(getLabel(getNodeId(path), paramName));
        instruction.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
        instruction.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
        instruction.setSubject(new PartySelf());

        if (map.containsKey(paramName)) {
            List<Activity> activities = new ArrayList<>();
            processAttributeChildren(oap, paramName, activities, (Map<String, Object>) map.get(paramName));
            instruction.setActivities(activities);
            if (oa) {
                ((ArrayList<ContentItem>) jsonmap).add(instruction);
            }
        }
    }

    public static void gen_ACTION(String path, String name, Object jsonmap, Map<String, Object> map)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        // String newPath = path + "/attributes";
        Action action = new Action();
        action.setArchetypeNodeId(nodeId);

        action.setNameAsString(label);

        // events.setTime(new DvDateTime((String) map.get("events_time")));
        // ItemTree itemTree = new ItemTree();
        // processAttributeChildren(path, nodeId, itemTree, map);
        // section.setItems(itemTree);

        // ((History<ItemStructure>) jsonmap).addEvent(events);
    }

    // INSTRUCTION_DETAILS

    // ISM_TRANSITION

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
        String archeTypeId = getArcheTypeId(path + "/../../../../../..");
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
        processAttributeChildren(newPath, archeTypeId, items, map);
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
        String code = !"".equals(paramName) ? paramName : nodeId;
        if (!map.containsKey(code)) {
            return;
        }
        String label = getLabel(nodeId, aNodeId);
        Cluster cluster = new Cluster();
        cluster.setArchetypeNodeId(aNodeId);
        cluster.setNameAsString(label);
        ArrayList<Item> items = new ArrayList<Item>();
        Generator.processAttributeChildren(newPath, paramName, items, (Map<String, Object>) map.get(code));
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

        Element el = new Element();
        el.setArchetypeNodeId(nodeId);
        el.setNameAsString(getLabel(nodeId, name));
        switch (map.get(nodeId)) {
            case DataValue d -> el.setValue(d);
            default -> processAttributeChildren(newPath, nodeId, el, map);
        }
        ((ArrayList<Element>) jsonmap).add(el);
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
        ((Element) jsonmap).setValue(new DvText(map.get(name)));
    }

    // TERM_MAPPING

    // CODE_PHRASE

    public static void gen_DV_CODED_TEXT(String path, String name, Object jsonmap,
            Map<String, Coding> map) throws Exception {
        Coding coding = map.get(name);
        DvCodedText ct = new DvCodedText();
        ct.setDefiningCode(new CodePhrase(
                new TerminologyId("terminology://fhir.hl7.org//ValueSet/$expand?url=" + coding.getSystem(),
                        coding.getVersion()),
                coding.getCode()));
        ct.setValue(coding.getCode());
        ((Element) jsonmap).setValue(ct);

    }

    // DV_PARAGRAPH

    // Quantity Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_3

    // DV_INTERVAL

    // REFERENCE_RANGE

    // DV_ORDINAL

    // DV_SCALE

    // DV_QUANTIFIED

    // DV_AMOUNT

    // DV_QUANTITY

    public static void gen_DV_COUNT(String path, String name, Object jsonmap,
            Map<String, Long> map) {
        ((Element) jsonmap).setValue(new DvCount(map.get(name)));
    }

    // DV_PROPORTION

    // PROPORTION_KIND

    // DV_ABSOLUTE_QUANTITY

    // DateTime Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_4

    // DV_DATE

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

    private static String getLabel(String code, String archetype) throws Exception {
        String path = "/template/definition/attributes[rm_attribute_name=\"content\"]//children[archetype_id/value=\""
                + archetype + "\"]/term_definitions[@code=\"" + code + "\"]/items[@id=\"text\"]";
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

}
