package de.uksh.medic.etl.openehrmapper;

import com.nedap.archie.rm.archetyped.Archetyped;
import com.nedap.archie.rm.archetyped.TemplateId;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.composition.ContentItem;
import com.nedap.archie.rm.composition.EventContext;
import com.nedap.archie.rm.datastructures.ItemTree;
import com.nedap.archie.rm.datatypes.CodePhrase;
import com.nedap.archie.rm.datavalues.DvCodedText;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDateTime;
import com.nedap.archie.rm.generic.PartySelf;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.TerminologyId;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Map;
import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.joda.time.LocalDateTime;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class EHRParser {
    public Composition build(String xml, Map<String, Object> map)
            throws ParserConfigurationException, SAXException, XPathExpressionException, IOException,
            JAXBException {

        String pathContent = "/template/definition[rm_type_name = \"COMPOSITION\"]"
                + "/attributes[rm_attribute_name=\"content\"]";
        String pathContext = "/template/definition[rm_type_name = \"COMPOSITION\"]"
                + "/attributes[rm_attribute_name=\"context\"]/children/attributes[rm_attribute_name=\"other_context\"]";
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        XPathFactory xpf = XPathFactory.newInstance();
        XPath xp = xpf.newXPath();

        Composition composition = new Composition();

        factory.setNamespaceAware(false); // never forget this!
        DocumentBuilder builder = factory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(xml));
        Document doc = builder.parse(is);

        composition.setArchetypeNodeId(
                ((String) xp.evaluate("/template/definition/archetype_id", doc, XPathConstants.STRING))
                        .trim());

        composition.setNameAsString(
                ((String) xp.evaluate("/template/definition/template_id", doc, XPathConstants.STRING))
                        .trim());

        Archetyped archetypeDetails = new Archetyped();
        archetypeDetails.setArchetypeId(new ArchetypeID(
                ((String) xp.evaluate("/template/definition/archetype_id", doc, XPathConstants.STRING))
                        .trim()));

        TemplateId templateId = new TemplateId();
        templateId.setValue(((String) xp.evaluate("/template/template_id", doc, XPathConstants.STRING)).trim());
        archetypeDetails.setTemplateId(templateId);
        archetypeDetails.setRmVersion("1.1.0");
        composition.setArchetypeDetails(archetypeDetails);

        composition.setLanguage(new CodePhrase(new TerminologyId("ISO_639-1"), "de"));

        composition.setTerritory(new CodePhrase(new TerminologyId("ISO_3166-1"), "DE"));

        composition.setCategory(new DvCodedText("event", new CodePhrase(new TerminologyId("openehr"), "433")));

        composition.setComposer(new PartySelf());
        map.put("start_time", map.getOrDefault("start_time", LocalDateTime.now().toString()));

        Generator g = new Generator(doc);
        Map<String, Object> applyMap = Generator.applyDefaults(map);

        ArrayList<ContentItem> content = new ArrayList<ContentItem>();
        composition.setContent(content);
        Generator.processAttributeChildren(pathContent, composition.getArchetypeNodeId(), content, applyMap);

        EventContext context = new EventContext(new DvDateTime((String) map.get("start_time")),
                new DvCodedText("other care", new CodePhrase(new TerminologyId("openehr"), "238")));
        composition.setContext(context);
        ItemTree itemTree = new ItemTree();
        context.setOtherContext(itemTree);
        Generator.processAttributeChildren(pathContext, composition.getArchetypeNodeId(), itemTree, applyMap);

        return composition;

    }

}
