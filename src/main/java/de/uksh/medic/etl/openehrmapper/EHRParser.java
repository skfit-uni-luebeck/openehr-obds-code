package de.uksh.medic.etl.openehrmapper;

import com.nedap.archie.rm.archetyped.Archetyped;
import com.nedap.archie.rm.archetyped.FeederAudit;
import com.nedap.archie.rm.archetyped.FeederAuditDetails;
import com.nedap.archie.rm.archetyped.TemplateId;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.composition.ContentItem;
import com.nedap.archie.rm.composition.EventContext;
import com.nedap.archie.rm.datastructures.ItemTree;
import com.nedap.archie.rm.datatypes.CodePhrase;
import com.nedap.archie.rm.datavalues.DvCodedText;
import com.nedap.archie.rm.datavalues.DvIdentifier;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDateTime;
import com.nedap.archie.rm.generic.PartyIdentified;
import com.nedap.archie.rm.generic.PartySelf;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.TerminologyId;
import de.uksh.medic.etl.settings.Settings;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.joda.time.LocalDateTime;
import org.tinylog.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class EHRParser {

    private Document doc;
    private Generator g;

    public EHRParser(String xml) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false); // never forget this!
        try {
            doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
            g = new Generator(doc);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            Logger.error(e);
        }
    }

    public Composition build(Map<String, Object> map, Map<String, Object> datatypes)
            throws XPathExpressionException {

        String pathContent = "//template/definition[rm_type_name = \"COMPOSITION\"]"
                + "/attributes[rm_attribute_name=\"content\"]";
        String pathContext = "//template/definition[rm_type_name = \"COMPOSITION\"]"
                + "/attributes[rm_attribute_name=\"context\"]/children/attributes[rm_attribute_name=\"other_context\"]";

        XPathFactory xpf = XPathFactory.newInstance();
        XPath xp = xpf.newXPath();

        Composition composition = new Composition();

        composition.setArchetypeNodeId(
                ((String) xp.evaluate("//template/definition/archetype_id", doc, XPathConstants.STRING)).trim());

        String name = ((String) xp.evaluate(
                "//template/definition/attributes[rm_attribute_name = \"name\"]/children/attributes"
                        + "[rm_attribute_name = \"value\"]/children/item/list/text()",
                doc, XPathConstants.STRING)).trim();
        if ("".equals(name)) {
            name = ((String) xp.evaluate(
                    "//template/definition/template_id/value/text()",
                    doc, XPathConstants.STRING)).trim();
        }
        composition.setNameAsString(name);

        Archetyped archetypeDetails = new Archetyped();
        archetypeDetails.setArchetypeId(new ArchetypeID(
                ((String) xp.evaluate("//template/definition/archetype_id", doc, XPathConstants.STRING)).trim()));

        TemplateId templateId = new TemplateId();
        templateId.setValue(((String) xp.evaluate("//template/template_id", doc, XPathConstants.STRING)).trim());
        archetypeDetails.setTemplateId(templateId);
        archetypeDetails.setRmVersion("1.1.0");
        composition.setArchetypeDetails(archetypeDetails);
        composition.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));
        composition.setTerritory(new CodePhrase(new TerminologyId("ISO_3166-1"), "DE"));
        composition.setCategory(new DvCodedText("event", new CodePhrase(new TerminologyId("openehr"), "433")));
        composition.setComposer(new PartySelf());

        String systemId = map.containsKey("systemId") ? ((List<String>) map.get("systemId")).getFirst()
                : Settings.getSystemId();
        Logger.debug("Setting Feeder_Audit System-ID to {}", systemId);
        FeederAuditDetails details = new FeederAuditDetails(systemId);

        List<String> list = (List<String>) map.get("identifier");
        FeederAudit audit;

        if (list != null && !list.isEmpty()) {
            Logger.debug("Setting originating_system_item_id to {}", list.getFirst());
            DvIdentifier identifier = new DvIdentifier();
            identifier.setId(list.getFirst());
            audit = new FeederAudit(details, List.of(identifier), null, null, null);
        } else {
            audit = new FeederAudit(details, null, null, null, null);
        }

        composition.setFeederAudit(audit);

        map.put("start_time",
                ((List<String>) map.getOrDefault("start_time", List.of(LocalDateTime.now().toString()))).get(0));

        Map<String, Object> applyMap = g.applyDefaults(map);
        ArrayList<ContentItem> content = new ArrayList<>();
        composition.setContent(content);
        g.processAttributeChildren(pathContent, composition.getArchetypeNodeId(), content, applyMap, datatypes);

        EventContext context = new EventContext(new DvDateTime((String) map.get("start_time")),
                new DvCodedText("other care", new CodePhrase(new TerminologyId("openehr"), "238")));
        if (map.containsKey("end_time")) {
            context.setEndTime(new DvDateTime(((List<String>) map.get("end_time")).get(0)));
        }
        if (map.containsKey("health_care_facility")) {
            Map<String, List<String>> hcf = (Map<String, List<String>>) map.get("health_care_facility");
            DvIdentifier identifier = new DvIdentifier();
            identifier.setId(hcf.get("id").get(0));
            context.setHealthCareFacility(new PartyIdentified(null, hcf.get("name").get(0), List.of(identifier)));
        }
        composition.setContext(context);
        ItemTree itemTree = new ItemTree();
        context.setOtherContext(itemTree);
        g.processAttributeChildren(pathContext, composition.getArchetypeNodeId(), itemTree, applyMap, datatypes);

        return composition;

    }

}
