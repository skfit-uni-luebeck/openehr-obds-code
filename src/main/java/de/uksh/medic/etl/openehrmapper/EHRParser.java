package de.uksh.medic.etl.openehrmapper;

import com.nedap.archie.json.JacksonUtil;
import com.nedap.archie.rm.archetyped.Archetyped;
import com.nedap.archie.rm.archetyped.TemplateId;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.composition.ContentItem;
import com.nedap.archie.rm.composition.EventContext;
import com.nedap.archie.rm.datatypes.CodePhrase;
import com.nedap.archie.rm.datavalues.DvCodedText;
import com.nedap.archie.rm.datavalues.quantity.datetime.DvDateTime;
import com.nedap.archie.rm.generic.PartyIdentified;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.TerminologyId;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class EHRParser {
    public String build(Map<String, Object> map)
            throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
        String file = "oBDS_Tumorkonferenz.opt";
        String path = "/template/definition[rm_type_name = \"COMPOSITION\"]/attributes[rm_attribute_name=\"content\"]";
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        XPathFactory xpf = XPathFactory.newInstance();
        XPath xp = xpf.newXPath();

        Composition composition = new Composition();

        factory.setNamespaceAware(false); // never forget this!
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(new File(file));

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

        PartyIdentified composer = new PartyIdentified(null, "MyDoctor", null);
        composition.setComposer(composer);
        map.put("start_time", "20191122T101638,642+0000"); // need to fix
        composition.setContext(new EventContext(new DvDateTime((String) map.get("start_time")),
                new DvCodedText("other care", new CodePhrase(new TerminologyId("openehr"), "238"))));

        Generator g = new Generator(doc);
        ArrayList<ContentItem> content = new ArrayList<ContentItem>();
        Map<String, Object> applyMap = Generator.applyDefaults(map);
        composition.setContent(content);

        Generator.processAttributeChildren(path, "", content, applyMap);

        System.out.println("Finished JSON-Generation. Generating String.");

        String ehr = JacksonUtil.getObjectMapper().writeValueAsString(composition);
        return ehr;

    }

}
