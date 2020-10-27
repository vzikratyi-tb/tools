package test.com;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.thingsboard.rest.client.RestClient;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.group.EntityGroupInfo;
import org.thingsboard.server.common.data.id.EntityGroupId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;

import javax.annotation.PostConstruct;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.springframework.util.StringUtils.isEmpty;

@Service
public class RestTemplateService {
    @Value("${thingsboard.url}")
    private String url;
    @Value("${thingsboard.username}")
    private String username;
    @Value("${thingsboard.password}")
    private String password;

    @Autowired
    private JdbcTemplate template;
    private RestTemplate restTemplate;

    @PostConstruct
    public void init() throws Exception{
        trustSelfSignedSSL();
        RestClient restClient = new RestClient(createRestTemplate(), url);
        this.restTemplate = restClient.getRestTemplate();
        restClient.login(username, password);

        Optional<Customer> customerOpt = restClient.getTenantCustomer("");
        if (!customerOpt.isPresent()) {
            throw new RuntimeException("Cannot find customer.");
        }

        Map<String, Warehouse> warehouses = new HashMap<>();
        Map<String, Store> stores = new HashMap<>();
        Map<String, Device> devices = new HashMap<>();

        List<EntityGroupInfo> mainCustomerGroups = restClient.getEntityGroupsByOwnerAndType(customerOpt.get().getId(), EntityType.CUSTOMER);
        EntityGroupInfo warehouseGroup = mainCustomerGroups.stream()
                .filter(entityGroupInfo -> entityGroupInfo.getName().equals("Warehouses"))
                .findAny().orElseThrow(() -> new RuntimeException("Cannot find Warehouse group"));

        getGroupCustomers(warehouseGroup.getId(), new PageLink(1000)).getData()
                .forEach(warehouse -> warehouses.put(warehouse.getId().getId().toString(), new Warehouse(warehouse.getTitle())));

        String warehouseIds = StringUtils.join(warehouses.keySet().stream().map(s -> "'"+s+"'").collect(Collectors.toList()), ",");
        template.query("select c.id, c.parent_customer_id, c.title, attr.str_v from customer c, attribute_kv attr where attr.entity_id = c.id " +
                        "and c.parent_customer_id in (" + warehouseIds + ") " +
                        "and attr.entity_type = 'CUSTOMER' " +
                        "and attr.attribute_type = 'SERVER_SCOPE' " +
                        "and attr.attribute_key = 'address_city';",
                (resultSet, i) -> {
            String id = resultSet.getString("id");
            String title = resultSet.getString("title");
            String warehouseId = resultSet.getString("parent_customer_id");
            String city = resultSet.getString("str_v");
            stores.put(id, new Store(warehouseId, title, city));
            return new Object();
        });


        String storeIds = StringUtils.join(stores.keySet().stream().map(s -> "'"+s+"'").collect(Collectors.toList()), ",");
        template.query("select d.id, d.customer_id, d.name from device d, attribute_kv attr where attr.entity_id = d.id " +
                        "and d.customer_id in (" + storeIds + ") " +
                        "and attr.entity_type = 'DEVICE' " +
                        "and attr.attribute_type = 'CLIENT_SCOPE' " +
                        "and attr.attribute_key = 'location' " +
                        "and attr.str_v = 'Lager';",
                (resultSet, i) -> {
                    String id = resultSet.getString("id");
                    String name = resultSet.getString("name");
                    String storeId = resultSet.getString("customer_id");
                    devices.put(id, new Device(storeId, name));
                    return new Object();
                });


        try (PrintWriter writer = new PrintWriter(new FileWriter(new File("/home/viktor/Temp/stores.csv")))) {
            writer.println("Warehouse,City,Store,Device");
            devices.forEach((id, device) -> {
                Store store = stores.get(device.storeId);
                Warehouse warehouse = warehouses.get(store.warehouseId);
                writer.println(warehouse.title+ "," + store.city + "," + store.title + "," + device.name);
            });
        }
    }

    private RestTemplate createRestTemplate() {
        RestTemplate restTemplate = new RestTemplate(new SimpleClientHttpRequestFactory() {
            @Override
            protected void prepareConnection(HttpURLConnection connection, String httpMethod) throws IOException {
                if (connection instanceof HttpsURLConnection) {
                    ((HttpsURLConnection) connection).setHostnameVerifier(new NoopHostnameVerifier());
                }
                super.prepareConnection(connection, httpMethod);
            }
        });
        return restTemplate;
    }

    public static void trustSelfSignedSSL() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            X509TrustManager tm = new X509TrustManager() {

                public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }

                public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }

                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };
            ctx.init(null, new TrustManager[]{tm}, null);
            SSLContext.setDefault(ctx);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @AllArgsConstructor
    private static class Warehouse {
        private String title;
    }

    @AllArgsConstructor
    private static class Store {
        private String warehouseId;
        private String title;
        private String city;
    }

    @AllArgsConstructor
    private static class Device {
        private String storeId;
        private String name;
    }


    public PageData<Customer> getGroupCustomers(EntityGroupId entityGroupId, PageLink pageLink) {
        Map<String, String> params = new HashMap<>();
        params.put("entityGroupId", entityGroupId.getId().toString());
        addPageLinkToParam(params, pageLink);

        return restTemplate.exchange(
                url + "/api/entityGroup/{entityGroupId}/customers?" + getUrlParams(pageLink),
                HttpMethod.GET,
                HttpEntity.EMPTY,
                new ParameterizedTypeReference<PageData<Customer>>() {
                },
                params).getBody();
    }

    private void addPageLinkToParam(Map<String, String> params, PageLink pageLink) {
        params.put("pageSize", String.valueOf(pageLink.getPageSize()));
        params.put("page", String.valueOf(pageLink.getPage()));
        if (!isEmpty(pageLink.getTextSearch())) {
            params.put("textSearch", pageLink.getTextSearch());
        }
        if (pageLink.getSortOrder() != null) {
            params.put("sortProperty", pageLink.getSortOrder().getProperty());
            params.put("sortOrder", pageLink.getSortOrder().getDirection().name());
        }
    }

    private String getUrlParams(PageLink pageLink) {
        String urlParams = "pageSize={pageSize}&page={page}";
        if (!isEmpty(pageLink.getTextSearch())) {
            urlParams += "&textSearch={textSearch}";
        }
        if (pageLink.getSortOrder() != null) {
            urlParams += "&sortProperty={sortProperty}&sortOrder={sortOrder}";
        }
        return urlParams;
    }
}
