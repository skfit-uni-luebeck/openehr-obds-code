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
import com.nedap.archie.rm.datavalues.quantity.DvInterval;
import com.nedap.archie.rm.datavalues.quantity.DvOrdinal;
import com.nedap.archie.rm.datavalues.quantity.DvProportion;
import com.nedap.archie.rm.datavalues.quantity.DvQuantity;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDate;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDateTime;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDuration;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvTime;
import com.nedap.archie.rm.generic.PartySelf;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.TerminologyId;
import de.uksh.medic.etl.model.MappingAttributes;
import de.uksh.medic.etl.model.Violation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
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
import org.hl7.fhir.r4.model.Quantity;
import org.tinylog.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class Generator {

    private static final XPath XP = XPathFactory.newInstance().newXPath();
    private final Document opt;
    private final Map<String, String> cache = new HashMap<>();
    private final Map<String, NodeList> cacheNodeList = new LinkedHashMap<>();

    public Generator(Document opt) {
        this.opt = opt;
    }

    public void processAttributeChildren(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes, List<Violation> violations) {
        String newPath = path + "/children";
        if (!cacheNodeList.containsKey(newPath + "/rm_type_name")) {
            XPathExpression expr;
            try {
                expr = XP.compile(newPath + "/rm_type_name");
                cacheNodeList.put(newPath + "/rm_type_name", (NodeList) expr.evaluate(opt, XPathConstants.NODESET));
            } catch (XPathExpressionException e) {
                Logger.error(e);
            }
        }
        NodeList children = cacheNodeList.get(newPath + "/rm_type_name");
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) == null || children.item(i).getFirstChild() == null || map == null) {
                Logger.debug("Encountered null children");
                return;
            }
            String type = "gen_" + children.item(i).getFirstChild().getTextContent();
            if (datatypes != null && datatypes.containsKey(name) && datatypes.get(name) instanceof MappingAttributes
                    && ((MappingAttributes) datatypes.get(name)).getDatatype() != null) {
                String type2 = "gen_" + ((MappingAttributes) datatypes.get(name)).getDatatype();
                if (!type2.equals(type)) {
                    continue;
                }
            }
            if ("gen_STRING".equals(type)) {
                Logger.debug("Filtered out gen_STRING!");
                return;
            }
            type = type.replaceAll("[^A-Za-z_]+", "_").replace("POINT_", "").replace("_INTERVAL_DV_QUANTITY_",
                    "_QUANTITY");
            Method met;
            try {
                met = this.getClass().getMethod(type, String.class, String.class, Object.class,
                        Map.class, Map.class, List.class);
                met.invoke(this, newPath + "[" + (i + 1) + "]", name, jsonmap, map, datatypes, violations);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {
                Logger.error(e);
            }
        }
    }

    // Navigation Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/ehr.html#_class_descriptions_4

    @SuppressWarnings("unchecked")
    public void gen_SECTION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String label = getLabel(path, getNodeId(path), paramName);
        String resolvedPath = paramName + ", '" + label + "'";
        String newPath = path + "/attributes[rm_attribute_name=\"items\"]";
        Section section = new Section();
        section.setArchetypeNodeId(paramName);

        section.setNameAsString(label);
        List<ContentItem> items = new ArrayList<>();
        processAttributeChildren(newPath, paramName, items,
                (Map<String, Object>) map.getOrDefault(resolvedPath, map.get(paramName)),
                (Map<String, Object>) datatypes.getOrDefault(resolvedPath, datatypes.get(paramName)),
                violations);
        section.setItems(items);

        ((List<ContentItem>) jsonmap).add(section);
    }

    // Entry Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/ehr.html#_class_descriptions_5

    @SuppressWarnings("unchecked")
    public void gen_ADMIN_ENTRY(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"data\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (!map.containsKey(paramName)) {
            return;
        }

        if (map.get(paramName) instanceof List) {
            l = (List<Map<String, Object>>) map.get(paramName);
        } else {
            l = List.of((Map<String, Object>) map.get(paramName));
        }

        l.forEach(le -> {

            AdminEntry adminEntry = new AdminEntry();
            adminEntry.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
            adminEntry.setArchetypeNodeId(paramName);
            adminEntry.setNameAsString(label);
            adminEntry.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
            adminEntry.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
            adminEntry.setSubject(new PartySelf());

            ItemTree itemTree = new ItemTree();
            processAttributeChildren(oap, paramName, itemTree, le, (Map<String, Object>) map.get(paramName),
                    violations);
            adminEntry.setData(itemTree);
            if (oa) {
                ((ArrayList<ContentItem>) jsonmap).add(adminEntry);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void gen_OBSERVATION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"data\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);
        String oapProtocol = path + "/attributes[rm_attribute_name=\"protocol\"]";
        Boolean oaProtocol = (Boolean) XP.evaluate(oapProtocol, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (!map.containsKey(paramName)) {
            return;
        }

        if (map.get(paramName) instanceof List) {
            l = (List<Map<String, Object>>) map.get(paramName);
        } else {
            l = List.of((Map<String, Object>) map.get(paramName));
        }

        l.forEach(le -> {

            Observation observation = new Observation();
            observation.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
            observation.setArchetypeNodeId(paramName);
            observation.setNameAsString(label);
            observation.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
            observation.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
            observation.setSubject(new PartySelf());

            History<ItemStructure> history = new History<>();
            processAttributeChildren(oap, paramName, history, le,
                    (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()), violations);
            if (history.getEvents().size() > 0) {
                observation.setData(history);
            }
            ItemTree protocol = new ItemTree();
            processAttributeChildren(oapProtocol, paramName, protocol, le,
                    (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()), violations);
            if (protocol.getItems().size() > 0) {
                observation.setProtocol(protocol);
            }
            if (oa || oaProtocol) {
                ((List<ContentItem>) jsonmap).add(observation);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void gen_EVALUATION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"data\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);
        String oapProtocol = path + "/attributes[rm_attribute_name=\"protocol\"]";
        Boolean oaProtocol = (Boolean) XP.evaluate(oapProtocol, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (!map.containsKey(paramName)) {
            return;
        }

        if (map.get(paramName) instanceof List) {
            l = (List<Map<String, Object>>) map.get(paramName);
        } else {
            l = List.of((Map<String, Object>) map.get(paramName));
        }

        l.forEach(le -> {

            Evaluation evaluation = new Evaluation();
            evaluation.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
            evaluation.setArchetypeNodeId(paramName);
            evaluation.setNameAsString(label);
            evaluation.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
            evaluation.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
            evaluation.setSubject(new PartySelf());

            ItemTree data = new ItemTree();
            processAttributeChildren(oap, paramName, data, le,
                    (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()), violations);
            if (data.getItems().size() > 0) {
                evaluation.setData(data);
            }
            ItemTree protocol = new ItemTree();
            processAttributeChildren(oapProtocol, paramName, protocol, le,
                    (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()), violations);
            if (protocol.getItems().size() > 0) {
                evaluation.setProtocol(protocol);
            }
            if (oa || oaProtocol) {
                ((List<ContentItem>) jsonmap).add(evaluation);
            }
        });

    }

    @SuppressWarnings("unchecked")
    public void gen_INSTRUCTION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oapActivities = path + "/attributes[rm_attribute_name=\"activities\"]";
        String oapProtocol = path + "/attributes[rm_attribute_name=\"protocol\"]";
        Boolean oaActivities = (Boolean) XP.evaluate(oapActivities, opt, XPathConstants.BOOLEAN);
        Boolean oaProtocol = (Boolean) XP.evaluate(oapProtocol, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (!map.containsKey(paramName)) {
            return;
        }

        if (map.get(paramName) instanceof List) {
            l = (List<Map<String, Object>>) map.get(paramName);
        } else {
            l = List.of((Map<String, Object>) map.get(paramName));
        }

        l.forEach(le -> {

            Instruction instruction = new Instruction();
            instruction.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
            instruction.setArchetypeNodeId(paramName);
            instruction.setNameAsString(label);
            instruction.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
            instruction.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
            instruction.setSubject(new PartySelf());
            instruction.setNarrative(new DvText(((List<String>) le.get("narrative")).get(0)));

            List<Activity> activities = new ArrayList<>();
            ItemTree protocol = new ItemTree();
            processAttributeChildren(oapActivities, paramName, activities, le,
                    (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()), violations);
            processAttributeChildren(oapProtocol, paramName, protocol, le,
                    (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()), violations);
            if (activities.size() > 0) {
                instruction.setActivities(activities);
            }
            if (protocol.getItems().size() > 0) {
                instruction.setProtocol(protocol);
            }
            if (oaActivities || oaProtocol) {
                ((ArrayList<ContentItem>) jsonmap).add(instruction);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void gen_ACTIVITY(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String nodeId = getNodeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"description\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, nodeId, name);

        List<Map<String, Object>> l;
        if (map.containsKey(nodeId) && map.get(nodeId) instanceof List) {
            l = (List<Map<String, Object>>) map.get(nodeId);
        } else {
            l = List.of((Map<String, Object>) map.get(nodeId));
        }

        l.forEach(le -> {

            Activity activity = new Activity();
            activity.setActionArchetypeId("openEHR-EHR-INSTRUCTION.service_request.v1");
            activity.setArchetypeNodeId(nodeId);
            activity.setNameAsString(label);

            ItemTree itemTree = new ItemTree();
            processAttributeChildren(oap, name, itemTree, le,
                    (Map<String, Object>) datatypes.getOrDefault(nodeId, new HashMap<>()), violations);
            activity.setDescription(itemTree);
            if (oa) {
                ((ArrayList<Activity>) jsonmap).add(activity);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void gen_ACTION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oapDescription = path + "/attributes[rm_attribute_name=\"description\"]";
        String oapProtocol = path + "/attributes[rm_attribute_name=\"protocol\"]";
        Boolean oaDescription = (Boolean) XP.evaluate(oapDescription, opt, XPathConstants.BOOLEAN);
        Boolean oaProtocol = (Boolean) XP.evaluate(oapProtocol, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (!map.containsKey(paramName)) {
            return;
        }

        if (map.get(paramName) instanceof List) {
            l = (List<Map<String, Object>>) map.get(paramName);
        } else {
            l = List.of((Map<String, Object>) map.get(paramName));
        }

        l.forEach(le -> {
            Action action = new Action();
            action.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
            action.setArchetypeNodeId(paramName);
            action.setNameAsString(label);
            action.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
            action.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
            action.setSubject(new PartySelf());
            IsmTransition ism = new IsmTransition();
            ism.setCurrentState(
                    new DvCodedText("completed", new CodePhrase(new TerminologyId("openehr"), "532", "completed")));
            action.setIsmTransition(ism);
            action.setTime(new DvDateTime(((List<String>) le.get("time")).getFirst()));
            ItemTree description = new ItemTree();
            ItemTree protocol = new ItemTree();
            processAttributeChildren(oapDescription, paramName, description, le,
                    (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()), violations);
            processAttributeChildren(oapProtocol, paramName, protocol, le,
                    (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()), violations);
            if (description.getItems().size() > 0) {
                action.setDescription(description);
            }
            if (protocol.getItems().size() > 0) {
                action.setProtocol(protocol);
            }
            if (oaDescription || oaProtocol) {
                ((ArrayList<ContentItem>) jsonmap).add(action);
            }
        });

    }

    // Item Structure
    // https://specifications.openehr.org/releases/RM/latest/data_structures.html#_class_descriptions_2

    // ITEM_SINGLE

    // ITEM_LIST

    // ITEM_TABLE

    public void gen_ITEM_TREE(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        if (!cache.containsKey(path + "/../rm_attribute_name")) {
            XPathExpression expr = XP.compile(path + "/../rm_attribute_name");
            cache.put(path + "/../rm_attribute_name", (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        String attributeName = cache.get(path + "/../rm_attribute_name");
        String nodeId = getNodeId(path);
        String newPath = path + "/attributes";
        if ("".equals(attributeName)) {
            return;
        }

        ItemTree itemTree = (ItemTree) jsonmap;
        itemTree.setArchetypeNodeId(nodeId);
        itemTree.setNameAsString(getLabel(path, nodeId, name));
        ArrayList<Item> items = new ArrayList<>();
        itemTree.setItems(items);
        processAttributeChildren(newPath, name, items, map, datatypes, violations);
    }

    // Representation Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_structures.html#_class_descriptions_3

    @SuppressWarnings("unchecked")
    public void gen_CLUSTER(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        if (!cache.containsKey(path + "/archetype_id")) {
            XPathExpression expr = XP.compile(path + "/archetype_id");
            cache.put(path + "/archetype_id",
                    ((String) expr.evaluate(opt, XPathConstants.STRING)).trim());
        }
        String cAID = cache.get(path + "/archetype_id");
        String paramName = !"".equals(cAID) ? cAID : name;
        String nodeId = getNodeId(path);
        boolean isSlot = XP.evaluate(path + "/@type", opt, XPathConstants.STRING).equals("C_ARCHETYPE_ROOT");
        String aNodeId = isSlot ? paramName : nodeId;
        String newPath = path + "/attributes";
        String code = !"".equals(paramName) && !name.equals(paramName) ? paramName : nodeId;
        if (!map.containsKey(code)) {
            return;
        }
        String label = getLabel(path, nodeId, paramName);

        List<Map<String, Object>> l;
        String usedCode = map.containsKey(aNodeId) ? aNodeId : code;
        if (!map.containsKey(aNodeId)) {
            l = (List<Map<String, Object>>) map.get(code);
        } else if (map.get(aNodeId) instanceof List) {
            l = (List<Map<String, Object>>) map.get(aNodeId);
        } else {
            l = List.of((Map<String, Object>) map.get(aNodeId));
        }

        l.forEach(le -> {

            Cluster cluster = new Cluster();
            cluster.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
            cluster.setArchetypeNodeId(aNodeId);
            cluster.setNameAsString(label);
            ArrayList<Item> items = new ArrayList<>();
            Map<String, Object> mam = datatypes.containsKey(usedCode)
                    && datatypes.get(usedCode) instanceof MappingAttributes ? datatypes
                            : (Map<String, Object>) datatypes.getOrDefault(usedCode, new HashMap<>());
            processAttributeChildren(newPath, paramName, items, le, mam, violations);
            cluster.setItems(items);
            ((ArrayList<Object>) jsonmap).add(cluster);

        });
    }

    @SuppressWarnings("unchecked")
    public void gen_ELEMENT(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String nodeId = getNodeId(path);
        String newPath = path + "/attributes[rm_attribute_name = \"value\"]";
        String label = getElementLabel(path, nodeId, name);

        if (!map.containsKey(nodeId)) {
            if (isMandatory(path)) {
                Element el = new Element();
                el.setArchetypeNodeId(nodeId);
                el.setNameAsString(label);
                el.setNullFlavour(new DvCodedText("not applicable",
                        new CodePhrase(new TerminologyId("openehr"), "273", "not applicable")));
                ((ArrayList<Element>) jsonmap).add(el);

            }
            return;
        }

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
            processAttributeChildren(newPath, nodeId, el, mo, datatypes, violations);
            ((ArrayList<Element>) jsonmap).add(el);

        });
    }

    // HISTORY Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_structures.html#_class_descriptions_4

    @SuppressWarnings("unchecked")
    public void gen_HISTORY(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        if ("".equals(label)) {
            label = getLabel(path, nodeId, name);
        }
        String newPath = path + "/attributes";
        History<ItemStructure> history = (History<ItemStructure>) jsonmap;
        history.setArchetypeNodeId(nodeId);

        history.setNameAsString(label);

        history.setOrigin(new DvDateTime(((List<String>) map.get("events_time")).getFirst()));
        if (map.containsKey("events_end")) {
            ZonedDateTime zdt1 = ZonedDateTime.parse(((List<String>) map.get("events_time")).getFirst() + "Z");
            ZonedDateTime zdt2 = ZonedDateTime.parse(((List<String>) map.get("events_end")).getFirst() + "Z");
            history.setDuration(
                    new DvDuration((TemporalAmount) Period.between(zdt1.toLocalDate(), zdt2.toLocalDate())));
        }

        processAttributeChildren(newPath, name, history, map, datatypes, violations);
    }

    @SuppressWarnings("unchecked")
    public void gen_EVENT(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes, List<Violation> violations)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        if ("".equals(label)) {
            label = getLabel(path, nodeId, name);
        }
        String newPath = path + "/attributes";
        Event<ItemStructure> events = new PointEvent<>();
        events.setArchetypeNodeId(nodeId);

        events.setNameAsString(label);

        events.setTime(new DvDateTime(((List<String>) map.get("events_time")).getFirst()));
        ItemTree itemTree = new ItemTree();
        processAttributeChildren(newPath, name, itemTree, map, datatypes, violations);
        events.setData(itemTree);

        ((History<ItemStructure>) jsonmap).addEvent(events);
    }

    // Datatypes
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_data_types_information_model

    // Basic Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions

    public void gen_DV_BOOLEAN(String path, String name, Object jsonmap,
            Map<String, Boolean> map, Map<String, Object> datatypes, List<Violation> violations) {
        ((Element) jsonmap).setValue(new DvBoolean(map.get(name)));
    }

    // DV_STATE

    public void gen_DV_IDENTIFIER(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes, List<Violation> violations) {
        DvIdentifier id = new DvIdentifier();
        id.setId(String.valueOf(map.get(name)));
        ((Element) jsonmap).setValue(id);
    }

    // Text Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_2

    public void gen_DV_TEXT(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes, List<Violation> violations) throws Exception {
        if (!map.containsKey(name)) {
            return;
        } else if (map.get(name) instanceof Coding) {
            gen_DV_CODED_TEXT(path, name, jsonmap, map, datatypes, violations);
        } else {
            ((Element) jsonmap).setValue(new DvText((String) map.get(name)));
        }
    }

    // TERM_MAPPING

    // CODE_PHRASE

    public void gen_DV_CODED_TEXT(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes, List<Violation> violations) throws Exception {

        DvCodedText ct = new DvCodedText();
        switch (map.get(name)) {
            case Coding coding -> {
                ct.setDefiningCode(new CodePhrase(
                        new TerminologyId(coding.getSystem(), coding.getVersion()),
                        coding.getCode(), coding.getDisplay()));
                ct.setValue(coding.getDisplay());
                ((Element) jsonmap).setValue(ct);
            }
            case String s -> {
                String terminology = getLocalTerminologyId(path);
                String display = getLocalTerminologyTerm((String) map.get("name"), name, s);
                if ("".equals(display)) {
                    String local = getLocalTerm(path, s);
                    ct.setDefiningCode(new CodePhrase(
                            new TerminologyId(terminology),
                            local, s));
                    ct.setValue(s);
                } else {
                    ct.setDefiningCode(new CodePhrase(
                            new TerminologyId(terminology),
                            s, display));
                    ct.setValue(display);
                }

                ((Element) jsonmap).setValue(ct);
            }
            case DvCodedText dvct -> ((Element) jsonmap).setValue(dvct);
            case null -> {
            }
            default -> {
            }
        }

    }

    // public void gen_DV_PARAGRAPH(String path, String name, Object jsonmap,
    // Map<String, List<String>> map)
    // throws Exception {
    // if (!map.containsKey(name)) {
    // return;
    // }
    // List<DvText> dts = new ArrayList<>();
    // for(String s : map.get(name)) {
    // dts.add(new DvText(s));
    // }
    // DvParagraph dp = new DvParagraph();
    // dp.setItems(dts);
    // ((Element) jsonmap).setValue(dp);
    // }

    // Quantity Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_3

    public void gen_DV_INTERVAL(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes, List<Violation> violations) throws Exception {
        String[] intervals = map.get(name).split(" - ");
        if (intervals.length != 2) {
            return;
        }
        DvInterval<DvTime> dvi = new DvInterval<>();
        dvi.setLower(new DvTime(intervals[0]));
        dvi.setUpper(new DvTime(intervals[1]));
        ((Element) jsonmap).setValue(dvi);
    }

    // REFERENCE_RANGE

    public void gen_DV_ORDINAL(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes, List<Violation> violations) throws Exception {
        DvOrdinal dvo = new DvOrdinal();
        Long value = Long.valueOf(map.get(name));
        dvo.setValue(value);
        String ordinal = getOrdinal(path, map.get(name));
        String display = getLabel(path, ordinal, map.get("name"));
        DvCodedText ct = new DvCodedText(display, new CodePhrase(new TerminologyId("local"), ordinal, display));
        dvo.setSymbol(ct);
        ((Element) jsonmap).setValue(dvo);
    }
    // DV_SCALE

    // DV_QUANTIFIED

    // DV_AMOUNT

    @SuppressWarnings({ "MagicNumber" })
    public void gen_DV_QUANTITY(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes, List<Violation> violations) throws Exception {

        String unit = null;
        Double magnitude = null;
        Long precision = null;
        String unitDisplayName = null;

        switch (map.get(name)) {
            case String s -> {
                unit = "1";
                magnitude = Double.valueOf(s);
                precision = 1L;
            }
            case String[] m -> {
                String magnitudeS = m[0];
                if (magnitudeS == null || magnitudeS.isBlank()) {
                    return;
                }
                magnitude = Double.valueOf(magnitudeS);
                precision = -1L;
                if (m.length == 3) {
                    precision = Long.valueOf(m[2]);
                }
                unit = (String) m[1];
            }
            case Quantity q -> {
                if (q.getCode() == null) {
                    q.setCode("1");
                }
                unit = q.getCode();
                magnitude = q.getValue().doubleValue();
                precision = Long.valueOf(q.getValue().precision());
                if (q.getUnit() != null) {
                    unitDisplayName = q.getUnit();
                }
            }
            default -> {
            }
        }

        if (magnitude == null) {
            return;
        }

        if (isInBounds(path, unit, magnitude, violations)) {
            DvQuantity dvq = new DvQuantity(unit, magnitude, precision);
            if (unitDisplayName != null) {
                dvq.setUnitsDisplayName(unitDisplayName);
            }
            ((Element) jsonmap).setValue(dvq);
        }
    }

    public void gen_DV_COUNT(String path, String name, Object jsonmap,
            Map<String, Long> map, Map<String, Object> datatypes, List<Violation> violations) throws Exception {

        Long count = map.get(name);

        if (isInBoundsCount(path, count, violations)) {
            ((Element) jsonmap).setValue(new DvCount(count));
        }

    }

    public void gen_DV_PROPORTION(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes, List<Violation> violations) throws Exception {
        switch (map.get(name)) {
            case String[] s -> {
                if (isInBoundsProportion(path, Double.valueOf(s[0]), violations)) {
                    DvProportion dvp = new DvProportion(Double.valueOf(s[0]), Double.valueOf(s[1]), Long.valueOf(s[2]));
                    ((Element) jsonmap).setValue(dvp);
                }
            }
            default -> {
            }
        }
    }

    // PROPORTION_KIND

    // DV_ABSOLUTE_QUANTITY

    // DateTime Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_4

    public void gen_DV_DATE(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes, List<Violation> violations) {
        ((Element) jsonmap).setValue(new DvDate(map.get(name)));
    }

    public void gen_DV_TIME(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes, List<Violation> violations) {
        ((Element) jsonmap).setValue(new DvTime(map.get(name)));
    }

    public void gen_DV_DATE_TIME(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes, List<Violation> violations) {
        ((Element) jsonmap).setValue(new DvDateTime(map.get(name)));
    }

    public void gen_DV_DURATION(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes, List<Violation> violations) {
        ((Element) jsonmap).setValue(new DvDuration(map.get(name)));
    }

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

    public void gen_DV_URI(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes, List<Violation> violations) {
        ((Element) jsonmap).setValue(new DvURI(String.valueOf(map.get(name))));
    }

    // DV_EHR_URI

    // XPath Query functions

    private boolean isInBounds(String path, String unit, Double magnitude, List<Violation> violations)
            throws Exception {
        String lowerPathValue = path + "/list[units=\"" + unit + "\"]/magnitude/lower/text()";
        String lowerPathInclude = path + "/list[units=\"" + unit + "\"]/magnitude/lower_included/text()";
        String lowerPathUnbound = path + "/list[units=\"" + unit + "\"]/magnitude/lower_unbounded/text()";
        String upperPathValue = path + "/list[units=\"" + unit + "\"]/magnitude/upper/text()";
        String upperPathInclude = path + "/list[units=\"" + unit + "\"]/magnitude/upper_included/text()";
        String upperPathUnbound = path + "/list[units=\"" + unit + "\"]/magnitude/upper_unbounded/text()";
        if (!cache.containsKey(lowerPathValue)) {
            XPathExpression expr = XP.compile(lowerPathValue);
            cache.put(lowerPathValue, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(lowerPathInclude)) {
            XPathExpression expr = XP.compile(lowerPathInclude);
            cache.put(lowerPathInclude, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(lowerPathUnbound)) {
            XPathExpression expr = XP.compile(lowerPathUnbound);
            cache.put(lowerPathUnbound, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathValue)) {
            XPathExpression expr = XP.compile(upperPathValue);
            cache.put(upperPathValue, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathInclude)) {
            XPathExpression expr = XP.compile(upperPathInclude);
            cache.put(upperPathInclude, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathUnbound)) {
            XPathExpression expr = XP.compile(upperPathUnbound);
            cache.put(upperPathUnbound, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        Double lowerValue = "".equals(cache.get(lowerPathValue)) ? null : Double.valueOf(cache.get(lowerPathValue));
        boolean lowerInclude = "".equals(cache.get(lowerPathInclude)) ? null
                : Boolean.parseBoolean(cache.get(lowerPathInclude));
        boolean lowerUnbound = "".equals(cache.get(lowerPathUnbound)) ? true
                : Boolean.parseBoolean(cache.get(lowerPathUnbound));

        Double upperValue = "".equals(cache.get(upperPathValue)) ? null : Double.valueOf(cache.get(upperPathValue));
        boolean upperInclude = "".equals(cache.get(upperPathInclude)) ? null
                : Boolean.parseBoolean(cache.get(upperPathInclude));
        boolean upperUnbound = "".equals(cache.get(upperPathUnbound)) ? true
                : Boolean.parseBoolean(cache.get(upperPathUnbound));

        boolean lower = lowerUnbound || lowerValue == null
                || (lowerInclude ? lowerValue <= magnitude : lowerValue < magnitude);

        boolean upper = upperUnbound || upperValue == null
                || (upperInclude ? magnitude <= upperValue : magnitude < upperValue);

        if (!lower || !upper) {
            violations.add(new Violation(magnitude, lower, upper, unit));
            String lowerSign = lowerInclude ? "[" : "(";
            String upperSign = upperInclude ? "]" : ")";
            Logger.error("Value " + magnitude + unit + " is not between bounds " + lowerSign + lowerValue + " - "
                    + upperValue + upperSign);
        }

        return lower && upper;

    }

    private boolean isInBoundsCount(String path, Long count, List<Violation> violations) throws Exception {
        String lowerPathValue = path
                + "/attributes[rm_attribute_name = \"magnitude\"]/children/item/range/lower/text()";
        String lowerPathInclude = path
                + "/attributes[rm_attribute_name = \"magnitude\"]/children/item/range/lower_included/text()";
        String lowerPathUnbound = path
                + "/attributes[rm_attribute_name = \"magnitude\"]/children/item/range/lower_unbounded/text()";
        String upperPathValue = path
                + "/attributes[rm_attribute_name = \"magnitude\"]/children/item/range/upper/text()";
        String upperPathInclude = path
                + "/attributes[rm_attribute_name = \"magnitude\"]/children/item/range/upper_included/text()";
        String upperPathUnbound = path
                + "/attributes[rm_attribute_name = \"magnitude\"]/children/item/range/upper_unbounded/text()";
        if (!cache.containsKey(lowerPathValue)) {
            XPathExpression expr = XP.compile(lowerPathValue);
            cache.put(lowerPathValue, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(lowerPathInclude)) {
            XPathExpression expr = XP.compile(lowerPathInclude);
            cache.put(lowerPathInclude, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(lowerPathUnbound)) {
            XPathExpression expr = XP.compile(lowerPathUnbound);
            cache.put(lowerPathUnbound, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathValue)) {
            XPathExpression expr = XP.compile(upperPathValue);
            cache.put(upperPathValue, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathInclude)) {
            XPathExpression expr = XP.compile(upperPathInclude);
            cache.put(upperPathInclude, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathUnbound)) {
            XPathExpression expr = XP.compile(upperPathUnbound);
            cache.put(upperPathUnbound, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        Long lowerValue = "".equals(cache.get(lowerPathValue)) ? null : Long.valueOf(cache.get(lowerPathValue));
        boolean lowerInclude = "".equals(cache.get(lowerPathInclude)) ? null
                : Boolean.parseBoolean(cache.get(lowerPathInclude));
        boolean lowerUnbound = "".equals(cache.get(lowerPathUnbound)) ? true
                : Boolean.parseBoolean(cache.get(lowerPathUnbound));

        Long upperValue = "".equals(cache.get(upperPathValue)) ? null : Long.valueOf(cache.get(upperPathValue));
        boolean upperInclude = "".equals(cache.get(upperPathInclude)) ? null
                : Boolean.parseBoolean(cache.get(upperPathInclude));
        boolean upperUnbound = "".equals(cache.get(upperPathUnbound)) ? true
                : Boolean.parseBoolean(cache.get(upperPathUnbound));

        boolean lower = lowerUnbound || lowerValue == null
                || (lowerInclude ? lowerValue <= count : lowerValue < count);

        boolean upper = upperUnbound || upperValue == null
                || (upperInclude ? count <= upperValue : count < upperValue);

        if (!lower || !upper) {
            violations.add(new Violation(count, lower, upper));
            String lowerSign = lowerInclude ? "[" : "(";
            String upperSign = upperInclude ? "]" : ")";
            Logger.error("Value " + count + " is not between bounds " + lowerSign + lowerValue + " - " + upperValue
                    + upperSign);
        }

        return lower && upper;

    }

    private boolean isInBoundsProportion(String path, Double numerator, List<Violation> violations) throws Exception {
        String lowerPathValue = path
                + "/attributes[rm_attribute_name = \"numerator\"]/children/item/range/lower/text()";
        String lowerPathInclude = path
                + "/attributes[rm_attribute_name = \"numerator\"]/children/item/range/lower_included/text()";
        String lowerPathUnbound = path
                + "/attributes[rm_attribute_name = \"numerator\"]/children/item/range/lower_unbounded/text()";
        String upperPathValue = path
                + "/attributes[rm_attribute_name = \"numerator\"]/children/item/range/upper/text()";
        String upperPathInclude = path
                + "/attributes[rm_attribute_name = \"numerator\"]/children/item/range/upper_included/text()";
        String upperPathUnbound = path
                + "/attributes[rm_attribute_name = \"numerator\"]/children/item/range/upper_unbounded/text()";
        if (!cache.containsKey(lowerPathValue)) {
            XPathExpression expr = XP.compile(lowerPathValue);
            cache.put(lowerPathValue, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(lowerPathInclude)) {
            XPathExpression expr = XP.compile(lowerPathInclude);
            cache.put(lowerPathInclude, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(lowerPathUnbound)) {
            XPathExpression expr = XP.compile(lowerPathUnbound);
            cache.put(lowerPathUnbound, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathValue)) {
            XPathExpression expr = XP.compile(upperPathValue);
            cache.put(upperPathValue, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathInclude)) {
            XPathExpression expr = XP.compile(upperPathInclude);
            cache.put(upperPathInclude, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!cache.containsKey(upperPathUnbound)) {
            XPathExpression expr = XP.compile(upperPathUnbound);
            cache.put(upperPathUnbound, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        Double lowerValue = "".equals(cache.get(lowerPathValue)) ? null : Double.valueOf(cache.get(lowerPathValue));
        boolean lowerInclude = "".equals(cache.get(lowerPathInclude)) ? null
                : Boolean.parseBoolean(cache.get(lowerPathInclude));
        boolean lowerUnbound = "".equals(cache.get(lowerPathUnbound)) ? true
                : Boolean.parseBoolean(cache.get(lowerPathUnbound));

        Double upperValue = "".equals(cache.get(upperPathValue)) ? null : Double.valueOf(cache.get(upperPathValue));
        boolean upperInclude = "".equals(cache.get(upperPathInclude)) ? null
                : Boolean.parseBoolean(cache.get(upperPathInclude));
        boolean upperUnbound = "".equals(cache.get(upperPathUnbound)) ? true
                : Boolean.parseBoolean(cache.get(upperPathUnbound));

        boolean lower = lowerUnbound || lowerValue == null
                || (lowerInclude ? lowerValue <= numerator : lowerValue < numerator);

        boolean upper = upperUnbound || upperValue == null
                || (upperInclude ? numerator <= upperValue : numerator < upperValue);

        if (!lower || !upper) {
            violations.add(new Violation(numerator, lower, upper));
            String lowerSign = lowerInclude ? "[" : "(";
            String upperSign = upperInclude ? "]" : ")";
            Logger.error("Value numerator " + numerator + " is not between bounds " + lowerSign + lowerValue + " - "
                    + upperValue
                    + upperSign);
        }

        return lower && upper;

    }

    private Boolean isMandatory(String path) throws Exception {
        String newPath = path + "/occurrences/lower/text()";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return "1".equals(cache.get(newPath));
    }

    private String getOrdinal(String path, String code) throws Exception {
        String newPath = path + "/list[value/text()=\"" + code + "\"]/symbol/defining_code/code_string/text()";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return cache.get(newPath);
    }

    private String getLabel(String path, String code, String archetype) throws Exception {

        String overridePath = path
                + "/attributes[rm_attribute_name=\"name\"]/children/attributes/children/item/list/text()";
        if (!cache.containsKey(overridePath)) {
            XPathExpression expr = XP.compile(overridePath);
            cache.put(overridePath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!"".equals(cache.get(overridePath))) {
            return cache.get(overridePath);
        }

        String newPath = "//archetype_id[value=\"" + archetype + "\"]/../term_definitions[@code=\"" + code
                + "\"]/items[@id=\"text\"]/text()";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return cache.get(newPath);
    }

    private String getElementLabel(String path, String code, String archetype) throws Exception {
        String overridePath = path
                + "/attributes[rm_attribute_name=\"name\"]/children/attributes/children/item/list/text()";
        if (!cache.containsKey(overridePath)) {
            XPathExpression expr = XP.compile(overridePath);
            cache.put(overridePath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!"".equals(cache.get(overridePath))) {
            return cache.get(overridePath);
        }

        String newPath = path + "/../../term_definitions[@code=\"" + code
                + "\"]/items[@id=\"text\"]/text()";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if ("".equals(cache.get(newPath))) {
            return getElementLabelLoop(path + "/..", code, archetype);
        }
        return cache.get(newPath);
    }

    private String getElementLabelLoop(String path, String code, String archetype) throws Exception {
        String newPath = path + "/../../term_definitions[@code=\"" + code
                + "\"]/items[@id=\"text\"]/text()";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if ("".equals(cache.get(newPath))) {
            return getElementLabelLoop(path + "/..", code, archetype);
        }
        return cache.get(newPath);
    }

    private String getTypeLabel(String path, String code) throws Exception {
        String newPath = path + "/term_definitions[@code=\"" + code + "\"]/items[@id=\"text\"]";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        if (!"".equals(cache.get(newPath))) {
            return cache.get(newPath);
        }

        newPath = path + "/../../../../term_definitions[@code=\"" + code + "\"]/items[@id=\"text\"]";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return cache.get(newPath);

    }

    private String getLocalTerminologyTerm(String archetype, String nodeId, String code) throws Exception {
        String newPath = "//archetype_id[value/text()=\"" + archetype + "\"]/../term_definitions[contains(@code, \"::"
                + code + "\")]/items/text()";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, ((String) expr.evaluate(opt, XPathConstants.STRING)).replaceAll("^* (?m) ", "")
                    .replaceAll("\\n", " "));
        }
        return cache.get(newPath);
    }

    private String getLocalTerminologyId(String path) throws Exception {
        String newPath = path + "/attributes/children[rm_type_name=\"CODE_PHRASE\"]/terminology_id/value/text()";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return cache.get(newPath);
    }

    private String getLocalTerm(String path, String code) throws Exception {
        String allowedCodesPath = path + "/attributes/children/code_list";
        XPathExpression expr = XP.compile(allowedCodesPath);
        NodeList nl = (NodeList) expr.evaluate(opt, XPathConstants.NODESET);
        List<String> codes = new ArrayList<>();
        for (int i = 0; i < nl.getLength(); i++) {
            codes.add(nl.item(i).getTextContent());
        }

        String codePath = "//term_definitions[items/@id=\"text\"][items/text()=\"" + code + "\"]/@code";
        XPathExpression expr2 = XP.compile(codePath);
        String nodeCode = (String) expr2.evaluate(opt, XPathConstants.STRING);
        for (String s : codes) {
            if (s.contains(nodeCode)) {
                return s;
            }
        }

        return null;
    }

    private String getNodeId(String path) throws Exception {
        String newPath = path + "/node_id";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return cache.get(newPath);
    }

    private String getArcheTypeId(String path) throws Exception {
        String newPath = path + "/archetype_id/value";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            cache.put(newPath, (String) expr.evaluate(opt, XPathConstants.STRING));
        }
        return cache.get(newPath);
    }

    public Map<String, Object> getDefaultValues() {
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
                List<String> l = List.of(differentialPath.split("]/"));
                String last = l.getLast().split("(\\[|\\])")[1].replaceAll(",.*", "");
                for (int j = 1; j < l.size() - 1; j++) {
                    String s = l.get(j) + "]";
                    if (s.contains("description[at") || s.contains("data[at")) {
                        continue;
                    }
                    Map<String, Object> n = new HashMap<>();
                    String s2 = s.split("(\\[|\\])")[1].replaceAll(",.*", "").split(" ")[0];
                    current.put(s2, n);
                    current = n;
                }
                List<String> ls = new ArrayList<>();
                ls.add(value);
                current.put(last, ls);
            }
        } catch (XPathExpressionException e) {
            Logger.error(e);
        }
        return defaults;
    }

    public Map<String, Object> applyDefaults(Map<String, Object> map) {
        Map<String, Object> defaults = getDefaultValues();
        deepMergeReplace(defaults, map);
        return defaults;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void deepMergeReplace(Map<String, Object> map1, Map<String, Object> map2) {
        for (String key : map2.keySet()) {
            Object value2 = map2.get(key);
            if (map1.containsKey(key)) {
                Object value1 = map1.get(key);
                if (value1 instanceof Map && value2 instanceof Map) {
                    deepMergeReplace((Map<String, Object>) value1, (Map<String, Object>) value2);
                } else if (value1 instanceof List && value2 instanceof List) {
                    map1.put(key, mergeReplace((List) value1, (List) value2));
                } else {
                    map1.put(key, value2);
                }
            } else {
                map1.put(key, value2);
            }
        }
    }

    public static void deepMergeNoReplace(Map<String, Object> map1, Map<String, Object> map2) {
        for (String key : map2.keySet()) {
            Object value2 = map2.get(key);
            if (map1.containsKey(key)) {
                Object value1 = map1.get(key);
                if (value1 instanceof Map && value2 instanceof Map) {
                    deepMergeNoReplace((Map<String, Object>) value1, (Map<String, Object>) value2);
                } else if (value1 instanceof List && value2 instanceof List) {
                    map1.put(key, mergeNoReplace((List) value2, (List) value1));
                } else {
                    map1.put(key, value2);
                }
            } else {
                map1.put(key, value2);
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List mergeReplace(List list1, List list2) {
        list1.clear();
        list1.addAll(list2);
        return list1;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List mergeNoReplace(List list1, List list2) {
        list1.addAll(list2);
        return list1;
    }

}
