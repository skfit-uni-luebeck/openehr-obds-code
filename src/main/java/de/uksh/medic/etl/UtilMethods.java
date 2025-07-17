package de.uksh.medic.etl;

import java.time.Period;
import java.time.ZonedDateTime;
import org.ehrbase.openehr.sdk.client.openehrclient.defaultrestclient.DefaultRestClient;
import org.ehrbase.openehr.sdk.generator.commons.aql.query.Query;
import org.ehrbase.openehr.sdk.response.dto.QueryResponseData;

public class UtilMethods {

    @SuppressWarnings("checkstyle:magicnumber")
    public String formatHl7DateTime(String date) {
        return date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8) + "T"
                + date.substring(8, 10) + ":" + date.substring(10, 12) + ":" + date.substring(12, 14);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public String getEncounterIdFromDate(DefaultRestClient openEhrClient, String ehrId, String date) {

        // Query encounter id from all compositions where source data contains encounter id
        String aql = "SELECT c1/context/other_context/items[openEHR-EHR-CLUSTER.case_identification.v0]"
                + "/items[at0001]/value/value AS encounterId,"
                + "c1/context/start_time/value AS dateTime "
                + "FROM EHR e "
                + "CONTAINS VERSION v "
                + "CONTAINS COMPOSITION c1 "
                + "CONTAINS CLUSTER enc[openEHR-EHR-CLUSTER.case_identification.v0] "
                + "WHERE e/ehr_status/subject/external_ref/id/value = '" + ehrId + "' "
                + "AND c1/name/value MATCHES {'KDS_Prozedur', 'Diagnose', 'KDS_Diagnose', "
                + "'Laborbericht', 'KDS_Laborbericht'} "
                + "AND c1/context/start_time/value <= '" + date + "' "
                + "ORDER BY c1/context/start_time/value DESC "
                + "LIMIT 1";

        QueryResponseData qrd = openEhrClient.aqlEndpoint().executeRaw(Query.buildNativeQuery(aql));

        // Return result if it less leq than 6 months old
        if (qrd != null && qrd.getRows() != null && qrd.getRows().size() > 0) {
            ZonedDateTime dateEncounter = ZonedDateTime.parse((String) qrd.getRows().getFirst().getLast() + "Z");
            ZonedDateTime dateComposition = ZonedDateTime.parse(date + "Z");
            Period p = Period.between(dateEncounter.toLocalDate(), dateComposition.toLocalDate());
            if (p.getYears() == 0 && p.getMonths() <= 6) {
                return (String) qrd.getRows().getFirst().getFirst();
            }
        }

        return null;

    }

}
