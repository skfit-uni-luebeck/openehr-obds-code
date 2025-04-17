package de.uksh.medic.etl;

import com.nedap.archie.rm.support.identification.ObjectVersionId;
import de.uksh.medic.etl.model.MappingAttributes;
import de.uksh.medic.etl.settings.Settings;
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

    protected static ObjectVersionId getVersionUid(DefaultRestClient openEhrClient, Map<String, MappingAttributes> aqls,
            String templateId, String itemId) throws ProcessingException {
        if (!aqls.containsKey(templateId) || aqls.get(templateId).getUpdateAql() == null) {
            Logger.warn("Cannot update composition because updateAql query not set.");
            return null;
        }
        QueryResponseData ehrIds = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(
                String.format(aqls.get(templateId).getUpdateAql(), templateId,
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
