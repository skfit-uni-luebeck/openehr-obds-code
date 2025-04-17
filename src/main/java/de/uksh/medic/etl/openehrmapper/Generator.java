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
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDuration;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvTime;
import com.nedap.archie.rm.generic.PartySelf;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.TerminologyId;

import de.uksh.medic.etl.model.MappingAttributes;

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
            Map<String, Object> map, Map<String, Object> datatypes) {
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
            if (datatypes != null && datatypes.containsKey(name) && datatypes.get(name) instanceof MappingAttributes && ((MappingAttributes) datatypes.get(name)).getDatatype() != null) {
                String type2 = "gen_" + ((MappingAttributes) datatypes.get(name)).getDatatype();
                if (!type2.equals(type)) {
                    continue;
                }
            }
            if ("gen_STRING".equals(type)) {
                Logger.debug("Filtered out gen_STRING!");
                return;
            }
            type = type.replaceAll("[^A-Za-z_]+", "_").replace("POINT_", "");
            Method met;
            try {
                met = this.getClass().getMethod(type, String.class, String.class, Object.class,
                        Map.class, Map.class);
                met.invoke(this, newPath + "[" + (i + 1) + "]", name, jsonmap, map, datatypes);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {
                Logger.error(e);
            }
        }
    }

    // Navigation Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/ehr.html#_class_descriptions_4

    @SuppressWarnings("unchecked")
    public void gen_SECTION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String label = getTypeLabel(path, getNodeId(path));
        String resolvedPath = paramName + ", '" + label + "'";
        String newPath = path + "/attributes";
        Section section = new Section();
        section.setArchetypeNodeId(paramName);

        section.setNameAsString(label);
        List<ContentItem> items = new ArrayList<>();
        processAttributeChildren(newPath, paramName, items,
                (Map<String, Object>) map.getOrDefault(resolvedPath, map.get(paramName)),
                (Map<String, Object>) datatypes.getOrDefault(resolvedPath, map.get(paramName)));
        section.setItems(items);

        ((List<ContentItem>) jsonmap).add(section);
    }

    // Entry Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/ehr.html#_class_descriptions_5

    @SuppressWarnings("unchecked")
    public void gen_ADMIN_ENTRY(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"data\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (map.containsKey(paramName) && map.get(paramName) instanceof List) {
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
            processAttributeChildren(oap, paramName, itemTree, le, (Map<String, Object>) map.get(paramName));
            adminEntry.setData(itemTree);
            if (oa) {
                ((ArrayList<ContentItem>) jsonmap).add(adminEntry);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void gen_OBSERVATION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oap = path + "/attributes[rm_attribute_name=\"data\"]";
        Boolean oa = (Boolean) XP.evaluate(oap, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (map.containsKey(paramName) && map.get(paramName) instanceof List) {
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
            processAttributeChildren(oap, paramName, history, le, (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()));
            observation.setData(history);
            if (oa) {
                ((List<ContentItem>) jsonmap).add(observation);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void gen_EVALUATION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
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

            Evaluation evaluation = new Evaluation();
            evaluation.setArchetypeDetails(new Archetyped(new ArchetypeID(paramName), "1.1.0"));
            evaluation.setArchetypeNodeId(paramName);
            evaluation.setNameAsString(label);
            evaluation.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
            evaluation.setEncoding(new CodePhrase(new TerminologyId("IANA_character-sets"), "UTF-8"));
            evaluation.setSubject(new PartySelf());

            ItemTree data = new ItemTree();
            processAttributeChildren(oap, paramName, data, le, (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()));
            evaluation.setData(data);
            if (oa) {
                ((List<ContentItem>) jsonmap).add(evaluation);
            }
        });

    }

    @SuppressWarnings("unchecked")
    public void gen_INSTRUCTION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oapActivities = path + "/attributes[rm_attribute_name=\"activities\"]";
        String oapProtocol = path + "/attributes[rm_attribute_name=\"protocol\"]";
        Boolean oaActivities = (Boolean) XP.evaluate(oapActivities, opt, XPathConstants.BOOLEAN);
        Boolean oaProtocol = (Boolean) XP.evaluate(oapProtocol, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (map.containsKey(paramName) && map.get(paramName) instanceof List) {
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
            instruction.setNarrative(new DvText(""));

            List<Activity> activities = new ArrayList<>();
            ItemTree protocol = new ItemTree();
            processAttributeChildren(oapActivities, paramName, activities, le, (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()));
            processAttributeChildren(oapProtocol, paramName, protocol, le, (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()));
            instruction.setActivities(activities);
            instruction.setProtocol(protocol);
            if (oaActivities || oaProtocol) {
                ((ArrayList<ContentItem>) jsonmap).add(instruction);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void gen_ACTIVITY(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
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
            processAttributeChildren(oap, name, itemTree, le, (Map<String, Object>) datatypes.getOrDefault(nodeId, new HashMap<>()));
            activity.setDescription(itemTree);
            if (oa) {
                ((ArrayList<Activity>) jsonmap).add(activity);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void gen_ACTION(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
            throws Exception {
        String paramName = getArcheTypeId(path);
        String oapDescription = path + "/attributes[rm_attribute_name=\"description\"]";
        String oapProtocol = path + "/attributes[rm_attribute_name=\"protocol\"]";
        Boolean oaDescription = (Boolean) XP.evaluate(oapDescription, opt, XPathConstants.BOOLEAN);
        Boolean oaProtocol = (Boolean) XP.evaluate(oapProtocol, opt, XPathConstants.BOOLEAN);
        String label = getLabel(path, getNodeId(path), paramName);

        List<Map<String, Object>> l;
        if (map.containsKey(paramName) && map.get(paramName) instanceof List) {
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
            processAttributeChildren(oapDescription, paramName, description, le, (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()));
            processAttributeChildren(oapProtocol, paramName, protocol, le, (Map<String, Object>) datatypes.getOrDefault(paramName, new HashMap<>()));
            action.setDescription(description);
            action.setProtocol(protocol);

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
            Map<String, Object> datatypes)
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
        itemTree.setNameAsString("data"); // fix name
        ArrayList<Item> items = new ArrayList<>();
        itemTree.setItems(items);
        processAttributeChildren(newPath, name, items, map, datatypes);
    }

    // Representation Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_structures.html#_class_descriptions_3

    @SuppressWarnings("unchecked")
    public void gen_CLUSTER(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes)
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
            cluster.setArchetypeNodeId(aNodeId);
            cluster.setNameAsString(label);
            ArrayList<Item> items = new ArrayList<>();
            processAttributeChildren(newPath, paramName, items, le, (Map<String, Object>) datatypes.getOrDefault(usedCode, new HashMap<>()));
            cluster.setItems(items);
            ((ArrayList<Object>) jsonmap).add(cluster);

        });
    }

    @SuppressWarnings("unchecked")
    public void gen_ELEMENT(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
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
            processAttributeChildren(newPath, nodeId, el, mo, datatypes);
            ((ArrayList<Element>) jsonmap).add(el);

        });
    }

    // HISTORY Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_structures.html#_class_descriptions_4

    @SuppressWarnings("unchecked")
    public void gen_HISTORY(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        String newPath = path + "/attributes";
        History<ItemStructure> history = (History<ItemStructure>) jsonmap;
        history.setArchetypeNodeId(nodeId);

        history.setNameAsString(label);

        history.setOrigin(new DvDateTime(((List<String>) map.get("events_time")).getFirst()));

        processAttributeChildren(newPath, nodeId, history, map, datatypes);
    }

    @SuppressWarnings("unchecked")
    public void gen_EVENT(String path, String name, Object jsonmap, Map<String, Object> map,
            Map<String, Object> datatypes)
            throws Exception {
        String nodeId = getNodeId(path);
        String label = getTypeLabel(path, nodeId);
        String newPath = path + "/attributes";
        Event<ItemStructure> events = new PointEvent<>();
        events.setArchetypeNodeId(nodeId);

        events.setNameAsString(label);

        events.setTime(new DvDateTime(((List<String>) map.get("events_time")).getFirst()));
        ItemTree itemTree = new ItemTree();
        processAttributeChildren(newPath, nodeId, itemTree, map, datatypes);
        events.setData(itemTree);

        ((History<ItemStructure>) jsonmap).addEvent(events);
    }

    // Datatypes
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_data_types_information_model

    // Basic Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions

    public void gen_DV_BOOLEAN(String path, String name, Object jsonmap,
            Map<String, Boolean> map, Map<String, Object> datatypes) {
        ((Element) jsonmap).setValue(new DvBoolean(map.get(name)));
    }

    // DV_STATE

    public void gen_DV_IDENTIFIER(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes) {
        DvIdentifier id = new DvIdentifier();
        id.setId(String.valueOf(map.get(name)));
        ((Element) jsonmap).setValue(id);
    }

    // Text Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_2

    public void gen_DV_TEXT(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes) throws Exception {
        if (!map.containsKey(name)) {
            return;
        } else if (map.get(name) instanceof Coding) {
            gen_DV_CODED_TEXT(path, name, jsonmap, map, datatypes);
        } else {
            ((Element) jsonmap).setValue(new DvText((String) map.get(name)));
        }
    }

    // TERM_MAPPING

    // CODE_PHRASE

    public void gen_DV_CODED_TEXT(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes) throws Exception {

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
                String display = getLocalTerminologyTerm((String) map.get("name"), name, s);
                if ("::".equals(display)) {
                    String local = getLocalTerm(path, s);
                    ct.setDefiningCode(new CodePhrase(
                            new TerminologyId("local"),
                            local, s));
                    ct.setValue(s);
                } else {
                    ct.setDefiningCode(new CodePhrase(
                            new TerminologyId(display.split("::")[0]),
                            s, display.split("::")[1]));
                    ct.setValue(display.split("::")[1]);
                }

                ((Element) jsonmap).setValue(ct);
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

    // DV_INTERVAL

    // REFERENCE_RANGE

    public void gen_DV_ORDINAL(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes) throws Exception {
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

    public void gen_DV_QUANTITY(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes) {

        switch (map.get(name)) {
            case String s -> {
                DvQuantity dvq = new DvQuantity("1", Double.valueOf(s), 1L);
                ((Element) jsonmap).setValue(dvq);
            }
            case String[] m -> {
                String magnitude = m[0];
                if (magnitude == null || magnitude.isBlank()) {
                    return;
                }
                Long precision = -1L;
                String units = (String) m[1];
                DvQuantity dvq = new DvQuantity(units, Double.valueOf(magnitude), precision);
                ((Element) jsonmap).setValue(dvq);
            }
            case Quantity q -> {
                DvQuantity dvq = new DvQuantity(q.getUnit(), q.getValue().doubleValue(),
                        Long.valueOf(q.getValue().precision()));
                ((Element) jsonmap).setValue(dvq);
            }
            default -> {
            }
        }
    }

    public void gen_DV_COUNT(String path, String name, Object jsonmap,
            Map<String, Long> map, Map<String, Object> datatypes) {
        ((Element) jsonmap).setValue(new DvCount(map.get(name)));
    }

    // DV_PROPORTION

    // PROPORTION_KIND

    // DV_ABSOLUTE_QUANTITY

    // DateTime Class descriptions
    // https://specifications.openehr.org/releases/RM/latest/data_types.html#_class_descriptions_4

    public void gen_DV_DATE(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes) {
        ((Element) jsonmap).setValue(new DvDate(map.get(name)));
    }

    public void gen_DV_TIME(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes) {
        ((Element) jsonmap).setValue(new DvTime(map.get(name)));
    }

    public void gen_DV_DATE_TIME(String path, String name, Object jsonmap,
            Map<String, String> map, Map<String, Object> datatypes) {
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

    public void gen_DV_URI(String path, String name, Object jsonmap,
            Map<String, Object> map, Map<String, Object> datatypes) {
        ((Element) jsonmap).setValue(new DvURI(String.valueOf(map.get(name))));
    }

    // DV_EHR_URI

    // XPath Query functions

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
            return getElementLabel(path + "/../..", code, archetype);
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
        String terminologyId = "//node_id[text()=\"" + nodeId + "\"]/../descendant::children[./code_list=\""
                + code + "\"]/terminology_id/value/text()";
        if (!cache.containsKey(newPath)) {
            XPathExpression expr = XP.compile(newPath);
            XPathExpression expr2 = XP.compile(terminologyId);
            cache.put(newPath, expr2.evaluate(opt, XPathConstants.STRING) + "::"
                    + ((String) expr.evaluate(opt, XPathConstants.STRING)).replaceAll("^* (?m) ", "")
                            .replaceAll("\\n", " "));
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
        NodeList nl2 = (NodeList) expr2.evaluate(opt, XPathConstants.NODESET);
        for (int i = 0; i < nl2.getLength(); i++) {
            if (codes.contains(nl2.item(i).getTextContent())) {
                return nl2.item(i).getTextContent();
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
                List<String> l = List.of(differentialPath.split("/"));
                String last = l.getLast().split("(\\[|\\])")[1].replaceAll(",.*", "");
                for (int j = 1; j < l.size() - 1; j++) {
                    String s = l.get(j);
                    if (s.contains("description[at") || s.contains("data[at")) {
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
            Logger.error(e);
        }
        return defaults;
    }

    public Map<String, Object> applyDefaults(Map<String, Object> map) {
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
        list1.addAll(list2);
        return list1;
    }

}
