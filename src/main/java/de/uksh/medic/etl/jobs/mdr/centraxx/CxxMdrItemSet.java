package de.uksh.medic.etl.jobs.mdr.centraxx;

import de.uksh.medic.etl.model.mdr.centraxx.*;
import de.uksh.medic.etl.settings.CxxMdrSettings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

/**
 * Class to query ItemSets from Kairos CentraXX MDR.
 */
public final class CxxMdrItemSet {

    private static final Map<String, CxxItemSet> CACHE = new HashMap<>();

    private CxxMdrItemSet() {
    }

    /**
     * Gets a specifc ItemSet from the MDR.
     * @param mdr Configuration for MDR.
     * @param itemSet Requested ItemSet
     * @return
     */
    public static CxxItemSet get(CxxMdrSettings mdr, String itemSet) {

        if (!CACHE.containsKey(itemSet)) {
            if (mdr.isTokenExpired()) {
                CxxMdrLogin.login(mdr);
            }
            RestTemplate rt = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON.toString());
            headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON.toString());
            headers.add("Authorization", "Bearer " + mdr.getToken());
            ResponseEntity<CxxItemSet> response = rt.exchange(
                    mdr.getUrl() + "/rest/v1/itemsets/itemset?code=" + itemSet, HttpMethod.GET,
                    new HttpEntity<>(headers), CxxItemSet.class);
            CACHE.put(itemSet, response.getBody());
        }
        return CACHE.get(itemSet);
    }

    /**
     * Helper method to get a list of items from an ItemSet.
     * @param itemSet CxxItemSet containing multiple CxxItems
     * @return List of CxxItem
     */
    public static List<CxxItem> getItemList(CxxItemSet itemSet) {
        return new ArrayList<>(itemSet.getItems());
    }

    /**
     * Helper method to get a list form items from a form.
     * @param form CxxForm containing CxxItems and CxxFields that contain CxxItems
     * @return List of CxxItem
     */
    public static List<CxxItem> getItemList(CxxForm form) {
        List<CxxItem> items = new ArrayList<>();
        for (CxxField f : form.getFields()) {
            items.add(f.getItem());
        }
        items.addAll(extractFromSections(form.getSections()));
        return items;
    }

    private static List<CxxItem> extractFromSections(List<CxxSection> sections) {
        List<CxxItem> items = new ArrayList<>();
        for (CxxSection s : sections) {
            for (CxxField f : s.getFields()) {
                items.add(f.getItem());
            }
            for (CxxSection subSection : sections) {
                extractFromSections(subSection.getSections());
            }
        }
        return items;
    }

}
