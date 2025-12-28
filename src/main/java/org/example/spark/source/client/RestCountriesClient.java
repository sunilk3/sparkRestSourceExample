package org.example.spark.source.client;

import javax.ws.rs.core.UriBuilder;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.example.spark.source.schema.Country;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestCountriesClient {
    private final String baseUrl;

    public RestCountriesClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public List<Country> Get(String path, Map<String,Object> queryParams ) {

        var uriBuilder = UriBuilder
                .fromUri(baseUrl);

        if(!"".equals(path)){
            uriBuilder.path(path);
        }

        if(!queryParams.isEmpty()){
            queryParams.forEach(uriBuilder::queryParam);
        }

        var uri = uriBuilder.build();
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            try {
                HttpGet httpGet = new HttpGet(uri);
                System.out.printf("Fetching data from: %s://%s%s", httpGet.getScheme(), httpGet.getAuthority(),httpGet.getRequestUri());

                // Execute the request and process the response entirely within the callback
                return httpClient.execute(httpGet, response -> {
                    if (response.getCode() != 200) {
                        throw new IOException("Unexpected response code: " + response.getCode());
                    }

                    // Consume the entity inside the execute block
                    String jsonResponse = EntityUtils.toString(response.getEntity());

                    // Map the JSON to objects inside the execute block
                    ObjectMapper objectMapper = new ObjectMapper();
                    var sourceCountries = objectMapper.readValue(jsonResponse, new TypeReference<List<org.example.spark.source.entity.Country>>(){});
                    var schemaCountries = new ArrayList<org.example.spark.source.schema.Country>();
                    sourceCountries.forEach( s -> {
                        var country = new Country();
                        country.name = s.name == null? "": s.name.common;
                        country.capital = s.capital == null || s.capital.isEmpty() ? "": s.capital.get(0);
                        country.region = s.region;
                        country.subregion = s.subregion;
                        country.area = s.area;
                        country.population = s.population;

                        schemaCountries.add(country);
                    });
                    return  schemaCountries;
                }); // Return the fully processed list

            } catch (IOException e) {
                e.printStackTrace(); // Handle or rethrow the exception properly
                return Collections.emptyList(); // Or throw a custom exception
            }
        } catch (IOException e) {
            e.printStackTrace(); // Handle URI build exceptions or client close exceptions
            return Collections.emptyList();
        }
    }
}
