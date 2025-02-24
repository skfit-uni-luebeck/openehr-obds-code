package de.uksh.medic.etl.jobs.mdr.centraxx;

import de.uksh.medic.etl.model.mdr.centraxx.MdrToken;
import de.uksh.medic.etl.settings.CxxMdrSettings;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.tinylog.Logger;

/**
 * Class handling the login into a Kairos CentraXX MDR.
 */
public final class CxxMdrLogin {

    private CxxMdrLogin() {
    }

    /**
     * Login method for Kairos CentraXX MDR.
     * @param mdr Configuration for MDR.
     * @return Updated configuration with access token and expiration timestamp.
     */
    public static CxxMdrSettings login(CxxMdrSettings mdr) {

        RestTemplate rt = new RestTemplate();
        rt.getInterceptors().add(new BasicAuthenticationInterceptor(mdr.getBasicUsername(), mdr.getBasicPassword()));
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.set("grant_type", "password");
        form.set("scope", "anyscope");
        form.set("username", mdr.getUsername());
        form.set("password", mdr.getPassword());
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(mdr.getUrl() + "/oauth/token");
        builder.queryParams(form);
        MdrToken token = rt.postForObject(builder.build().encode().toUri(), null, MdrToken.class);
        if (token != null) {
            Logger.warn("CXX MDR token not set. Login returned null.");
            mdr.setToken(token.getAccessToken(), token.getExpiresIn());
        }
        return mdr;
    }

}
