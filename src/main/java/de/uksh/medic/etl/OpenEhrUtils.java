package de.uksh.medic.etl;

import com.nedap.archie.rm.support.identification.ObjectVersionId;
import de.uksh.medic.etl.model.MappingAttributes;
import de.uksh.medic.etl.settings.Settings;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.ehrbase.openehr.sdk.client.openehrclient.defaultrestclient.DefaultRestClient;
import org.ehrbase.openehr.sdk.generator.commons.aql.query.Query;
import org.ehrbase.openehr.sdk.response.dto.QueryResponseData;
import org.tinylog.Logger;

public final class OpenEhrUtils {

    private OpenEhrUtils() {
    }

    protected static void deleteOpenEhrComposition(DefaultRestClient openEhrClient, Map<String, MappingAttributes> aqls,
            String templateId, String itemId) throws ProcessingException {
        if (aqls.get(templateId).getDeleteAql() == null) {
            Logger.warn("Cannot delete composition because deleteAql query not set.");
            return;
        }
        QueryResponseData ehrIds = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                String.format(aqls.get(templateId).getDeleteAql(), templateId,
                        Settings.getSystemId(), itemId)));
        if (ehrIds.getRows() == null) {
            Logger.info("Nothing to delete for templateId {}, originalId {} from system: {}",
                    templateId, itemId, Settings.getSystemId());
            return;
        }

        for (List<Object> l : ehrIds.getRows()) {
            UUID ehrId = UUID.fromString((String) l.getFirst());
            ObjectVersionId versionId = new ObjectVersionId((String) l.getLast());
            Logger.info("Deleting composition {} from ehr {}", versionId, ehrId);
            openEhrClient.compositionEndpoint(ehrId).delete(versionId);
        }
        return;
    }

    protected static Map<String, Object> getVersionUid(DefaultRestClient openEhrClient,
            Map<String, MappingAttributes> aqls,
            String templateId, String itemId) throws ProcessingException {

        Map<String, Object> oviMap = new HashMap<>();

        if (!aqls.containsKey(templateId) || aqls.get(templateId).getUpdateAql() == null) {
            Logger.warn("Cannot update composition because updateAql query not set.");
            return oviMap;
        }
        QueryResponseData ehrIds = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                String.format(aqls.get(templateId).getUpdateAql(), templateId,
                        Settings.getSystemId(), itemId)));
        if (ehrIds.getRows() == null) {
            Logger.info("No composition found for templateId {}, originalId {} from system: {}",
                    templateId, itemId, Settings.getSystemId());
            return oviMap;
        }

        if (ehrIds.getRows().size() > 1) {
            Logger.warn("Found more than one composition for ID: {} from system: {}!"
                    + " This should not happen! Deleting all but the latest.", itemId, Settings.getSystemId());
            for (int i = 1; i < ehrIds.getRows().size(); i++) {
                List<Object> l = ehrIds.getRows().get(i);
                UUID ehrId = UUID.fromString((String) l.getFirst());
                ObjectVersionId versionId = new ObjectVersionId((String) l.getLast());
                Logger.info("Deleting composition {} from ehr {}", versionId, ehrId);
                openEhrClient.compositionEndpoint(ehrId).delete(versionId);
            }
        }

        oviMap.put("ehr_id", UUID.fromString((String) ehrIds.getRows().getFirst().getFirst()));
        oviMap.put("ovi", new ObjectVersionId((String) ehrIds.getRows().getFirst().getLast()));

        return oviMap;
    }

}
