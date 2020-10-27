package test.com;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Precision;
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
import org.thingsboard.rest.client.utils.RestJsonConverter;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.group.EntityGroupInfo;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityGroupId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.page.TimePageLink;

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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.springframework.util.StringUtils.isEmpty;

@Service
public class RestTemplateService {
    private static final Long startTime = 1596578460000L; //05.08.2020 0:01
    private static final Long endTime = 1600207200000L; //16.09.2020 0:00
    private static final long numberOfDays = Math.round((double)(endTime - startTime) / TimeUnit.DAYS.toMillis(1 ));

    private static final int TEMPERATURE_VALUES_PER_DAY = 48;
    private static final int TIMESERIES_REQUEST_LIMIT = 10_000;

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
    public void init() throws Exception {
        trustSelfSignedSSL();
        RestClient restClient = new RestClient(createRestTemplate(), url);
        this.restTemplate = restClient.getRestTemplate();
        restClient.login(username, password);

        Optional<Customer> customerOpt = restClient.getTenantCustomer("");
        if (!customerOpt.isPresent()) {
            throw new RuntimeException("Cannot find customer.");
        }

        Map<String, Warehouse> warehouses = getWarehouses(restClient, customerOpt);
        Map<String, Store> stores = getWarehousesStores(warehouses);
        Map<String, Device> devices = getStoresDevices(stores);

        List<DeviceStats> devicesStats = new ArrayList<>();

        devices.forEach((deviceId, device) -> {
            Store store = stores.get(device.storeId);
            Warehouse warehouse = warehouses.get(store.warehouseId);

            List<String> keys = Arrays.asList("dailyAlarmTempMinutes", "dailyTemperatureAlarmsCounted", "temperature");
            List<TsKvEntry> timeseries = getTimeseries(new DeviceId(UUID.fromString(deviceId)), keys, 0L,
                    Aggregation.NONE, new TimePageLink(new PageLink(TIMESERIES_REQUEST_LIMIT), startTime, endTime), true);
            if (timeseries.size() >= TIMESERIES_REQUEST_LIMIT) {
                System.out.println("There are more telemetry than " + TIMESERIES_REQUEST_LIMIT);
            }

            List<TsKvEntry> temperatureEntries = timeseries.stream()
                    .filter(tsKvEntry -> "temperature".equals(tsKvEntry.getKey()) && tsKvEntry.getDoubleValue().isPresent())
                    .collect(Collectors.toList());
            List<TsKvEntry> dailyTemperatureAlarmsCountedEntries = timeseries.stream()
                    .filter(tsKvEntry -> "dailyTemperatureAlarmsCounted".equals(tsKvEntry.getKey()) && tsKvEntry.getLongValue().isPresent())
                    .collect(Collectors.toList());
            List<TsKvEntry> dailyAlarmTempMinutesEntries = timeseries.stream()
                    .filter(tsKvEntry -> "dailyAlarmTempMinutes".equals(tsKvEntry.getKey()) && tsKvEntry.getDoubleValue().isPresent())
                    .collect(Collectors.toList());

            long temperatureValuesCount = temperatureEntries.stream()
                    .map(KvEntry::getDoubleValue)
                    .mapToDouble(Optional::get)
                    .count();

            String maxTemperature;
            String avgTemperature;
            String avgOf5thPercentile;
            String avgOf1thPercentile;
            String temperatureLosses;
            String dailyAlarmTempMinutesSum = String.valueOf(Precision.round(
                    dailyAlarmTempMinutesEntries.stream()
                            .map(KvEntry::getDoubleValue)
                            .mapToDouble(Optional::get)
                            .sum(),
                    2));
            String dailyTemperatureAlarmsCountedSum = String.valueOf(
                    dailyTemperatureAlarmsCountedEntries.stream()
                            .map(KvEntry::getLongValue)
                            .mapToLong(Optional::get)
                            .sum());
            if (temperatureValuesCount == 0) {
                maxTemperature = "-";
                avgTemperature = "-";
                avgOf5thPercentile = "-";
                avgOf1thPercentile = "-";
                temperatureLosses = "100";
            } else {
                avgTemperature = String.valueOf(Precision.round(
                        temperatureEntries.stream()
                                .map(KvEntry::getDoubleValue)
                                .mapToDouble(Optional::get)
                                .average()
                                .orElse(0),
                        2));
                maxTemperature = String.valueOf(Precision.round(
                        temperatureEntries.stream()
                                .map(KvEntry::getDoubleValue)
                                .mapToDouble(Optional::get)
                                .max()
                                .orElse(0),
                        2));
                int count1thPercentile = (int) Math.ceil(temperatureValuesCount * 0.01);
                avgOf1thPercentile = String.valueOf(Precision.round(
                        temperatureEntries.stream()
                                .map(KvEntry::getDoubleValue)
                                .map(Optional::get)
                                .sorted(Collections.reverseOrder())
                                .limit(count1thPercentile)
                                .mapToDouble(value -> value)
                                .average()
                                .orElse(0),
                        2));
                int count5thPercentile = (int) Math.ceil(temperatureValuesCount * 0.05);
                avgOf5thPercentile = String.valueOf(Precision.round(
                        temperatureEntries.stream()
                                .map(KvEntry::getDoubleValue)
                                .map(Optional::get)
                                .sorted(Collections.reverseOrder())
                                .limit(count5thPercentile)
                                .mapToDouble(value -> value)
                                .average()
                                .orElse(0),
                        2));
                temperatureLosses = String.valueOf(Precision.round(
                        100 * (1 - (double) temperatureValuesCount / (TEMPERATURE_VALUES_PER_DAY * numberOfDays)), 2));
            }

            devicesStats.add(
                    DeviceStats.builder()
                            .storeName(store.title)
                            .city(store.city)
                            .warehouseName(warehouse.title)
                            .deviceName(device.name)
                            .maxTemperature(maxTemperature)
                            .avgTemperature(avgTemperature)
                            .avgOf5thPercentile(avgOf5thPercentile)
                            .avgOf1thPercentile(avgOf1thPercentile)
                            .temperatureLosses(temperatureLosses)
                            .dailyAlarmTempMinutesSum(dailyAlarmTempMinutesSum)
                            .dailyTemperatureAlarmsCountedSum(dailyTemperatureAlarmsCountedSum)
                            .build()
            );
        });

        try (PrintWriter writer = new PrintWriter(new FileWriter(new File("/home/viktor/Temp/stores_stats.csv")))) {
            writer.println("StoreName,City,WarehouseName,DeviceName,Tmax,Tavg," +
                    "5th percentile of max temp,1st percentile of max temp,temperature losses in %," +
                    "Kmin sum,number of temperature alarms");
            devicesStats.forEach(stats -> {
                writer.println(stats.storeName + "," + stats.city + "," + stats.warehouseName + "," + stats.deviceName +
                        "," + stats.maxTemperature + "," + stats.avgTemperature + "," + stats.avgOf5thPercentile +
                        "," + stats.avgOf1thPercentile + "," + stats.temperatureLosses + "," + stats.dailyAlarmTempMinutesSum +
                        "," + stats.dailyTemperatureAlarmsCountedSum);
            });
        }
    }

    private Map<String, Warehouse> getWarehouses(RestClient restClient, Optional<Customer> customerOpt) {
        Map<String, Warehouse> warehouses = new HashMap<>();
        List<EntityGroupInfo> mainCustomerGroups = restClient.getEntityGroupsByOwnerAndType(customerOpt.get().getId(), EntityType.CUSTOMER);
        EntityGroupInfo warehouseGroup = mainCustomerGroups.stream()
                .filter(entityGroupInfo -> entityGroupInfo.getName().equals("Warehouses"))
                .findAny().orElseThrow(() -> new RuntimeException("Cannot find Warehouse group"));

        getGroupCustomers(warehouseGroup.getId(), new PageLink(1000)).getData()
                .forEach(warehouse -> warehouses.put(warehouse.getId().getId().toString(), new Warehouse(warehouse.getTitle())));
        return warehouses;
    }

    private Map<String, Store> getWarehousesStores(Map<String, Warehouse> warehouses) {
        String warehouseIds = StringUtils.join(warehouses.keySet().stream().map(s -> "'" + s + "'").collect(Collectors.toList()), ",");
        Map<String, Store> stores = new HashMap<>();
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
        return stores;
    }

    private Map<String, Device> getStoresDevices(Map<String, Store> stores) {
        Map<String, Device> devices = new HashMap<>();
        String storeIds = StringUtils.join(stores.keySet().stream().map(s -> "'" + s + "'").collect(Collectors.toList()), ",");
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
        return devices;
    }

    @AllArgsConstructor
    @Builder
    private static class DeviceStats {
        private String storeName;
        private String city;
        private String warehouseName;
        private String deviceName;
        private String maxTemperature;
        private String avgTemperature;
        private String avgOf5thPercentile;
        private String avgOf1thPercentile;
        private String temperatureLosses;
        private String dailyAlarmTempMinutesSum;
        private String dailyTemperatureAlarmsCountedSum;
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


    public List<TsKvEntry> getTimeseries(EntityId entityId, List<String> keys, Long interval, Aggregation agg, TimePageLink pageLink, boolean useStrictDataTypes) {
        Map<String, String> params = new HashMap<>();
        params.put("entityType", entityId.getEntityType().name());
        params.put("entityId", entityId.getId().toString());
        params.put("keys", listToString(keys));
        params.put("interval", interval == null ? "0" : interval.toString());
        params.put("agg", agg == null ? "NONE" : agg.name());
        params.put("useStrictDataTypes", Boolean.toString(useStrictDataTypes));
        params.put("limit", String.valueOf(pageLink.getPageSize()));
        params.put("startTs", String.valueOf(pageLink.getStartTime()));
        params.put("endTs", String.valueOf(pageLink.getEndTime()));
        addPageLinkToParam(params, pageLink);

        Map<String, List<JsonNode>> timeseries = restTemplate.exchange(
                url + "/api/plugins/telemetry/{entityType}/{entityId}/values/timeseries?keys={keys}&interval={interval}&agg={agg}&useStrictDataTypes={useStrictDataTypes}&" + getUrlParamsTs(pageLink),
                HttpMethod.GET,
                HttpEntity.EMPTY,
                new ParameterizedTypeReference<Map<String, List<JsonNode>>>() {
                },
                params).getBody();

        return RestJsonConverter.toTimeseries(timeseries);
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

    private String listToString(List<String> list) {
        return String.join(",", list);
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

    private String getUrlParamsTs(TimePageLink pageLink) {
        return getUrlParams(pageLink, "startTs", "endTs");
    }

    private String getUrlParams(TimePageLink pageLink, String startTime, String endTime) {
        String urlParams = "limit={limit}";
        if (pageLink.getStartTime() != null) {
            urlParams += "&" + startTime + "={startTs}";
        }
        if (pageLink.getEndTime() != null) {
            urlParams += "&" + endTime + "={endTs}";
        }
        return urlParams;
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
}
