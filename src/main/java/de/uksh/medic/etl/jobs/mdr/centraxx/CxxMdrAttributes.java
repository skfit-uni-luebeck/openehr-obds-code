package de.uksh.medic.etl.jobs.mdr.centraxx;

import de.uksh.medic.etl.model.MappingAttributes;
import de.uksh.medic.etl.model.mdr.centraxx.CxxAttributeValue;
import de.uksh.medic.etl.model.mdr.centraxx.CxxList;
import de.uksh.medic.etl.settings.CxxMdrSettings;
import java.net.URI;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.tinylog.Logger;

/**
 * Class to fetch attributes from a Kairos CentraXX MDR.
 */
public final class CxxMdrAttributes {

    private CxxMdrAttributes() {
    }

    /**
     * Retrieves a FhirAttributes object for a specified MDR attribute.
     *
     * @param mdr        Configuration for MDR.
     * @param mdrProfile profile / form / ItemSet where the item is defined
     * @param key        key of the requested attribute
     * @return FhirAttributes object
     */
    public static MappingAttributes getAttributes(CxxMdrSettings mdr, String mdrProfile, String domain, String key) {

        if (mdr.isTokenExpired()) {
            CxxMdrLogin.login(mdr);
        }

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.set("code", mdrProfile);
        form.set("domainCode", domain);
        form.set("itemCode", key);

        RestTemplate rt = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        UriComponentsBuilder builder = UriComponentsBuilder
                .fromUriString(mdr.getUrl() + "/rest/v1/itemsets/attributes/item");
        builder.queryParams(form);
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON.toString());
        headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON.toString());
        headers.add("Authorization", "Bearer " + mdr.getToken());
        try {
            ResponseEntity<CxxList> response = rt.exchange(builder.build().encode().toUri(), HttpMethod.GET,
                    new HttpEntity<>(headers), CxxList.class);
            CxxList l = response.getBody();
            if (l != null && l.getContent() != null) {
                MappingAttributes ch = new MappingAttributes();
                for (CxxAttributeValue av : l.getContent()) {
                    if (av.getAttribute() == null) {
                        continue;
                    }
                    switch (av.getAttribute()) {
                        case SYSTEM -> ch.setSystem(URI.create(av.getValue()));
                        case SOURCE -> ch.setSource(URI.create(av.getValue()));
                        case TARGET -> ch.setTarget(URI.create(av.getValue()));
                        case ID -> ch.setId(av.getValue());
                        case CONCEPTMAP -> ch.setConceptMap(URI.create(av.getValue()));
                        case CODE -> ch.setCode(av.getValue());
                        case VERSION -> ch.setVersion(av.getValue());
                        case UNIT -> ch.setUnit(av.getValue());
                        case DATATYPE -> ch.setDatatype(av.getValue());
                        default -> {
                        }
                    }
                }
                return ch;
            }
            return null;

        } catch (final HttpClientErrorException e) {
            Logger.error("Object " + form.get("itemCode") + " not found in MDR!");
            return null;
        }
    }

    /**
     * Retrieves a FhirAttributes object for a specified MDR profiles.
     *
     * @param mdr        Configuration for MDR.
     * @param mdrProfile profile / form / ItemSet where the item is defined
     * @param key        key of the requested attribute
     * @return FhirAttributes object
     */
    public static MappingAttributes getProfileAttributes(CxxMdrSettings mdr, String mdrProfile, String domain) {

        if (mdr.isTokenExpired()) {
            CxxMdrLogin.login(mdr);
        }

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.set("code", mdrProfile);
        form.set("domainCode", domain);

        RestTemplate rt = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        UriComponentsBuilder builder = UriComponentsBuilder
                .fromUriString(mdr.getUrl() + "/rest/v1/itemsets/attributes/itemset");
        builder.queryParams(form);
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON.toString());
        headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON.toString());
        headers.add("Authorization", "Bearer " + mdr.getToken());
        try {
            ResponseEntity<CxxList> response = rt.exchange(builder.build().encode().toUri(), HttpMethod.GET,
                    new HttpEntity<>(headers), CxxList.class);
            CxxList l = response.getBody();
            if (l != null && l.getContent() != null) {
                MappingAttributes ch = new MappingAttributes();
                for (CxxAttributeValue av : l.getContent()) {
                    switch (av.getAttribute()) {
                        case UPDATEAQL -> ch.setUpdateAql(av.getValue());
                        case DELETEAQL -> ch.setDeleteAql(av.getValue());
                        default -> {
                        }
                    }
                }
                return ch;
            }
            return null;

        } catch (final HttpClientErrorException e) {
            Logger.error("Object " + form.get("itemCode") + " not found in MDR!");
            return null;
        }
    }

}
