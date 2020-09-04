package test.com;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class TestService {
    @Autowired
    private JdbcTemplate template;

    private long start = 1599184660000L;
//    private long start = 1599018600000L; // 02.09 3:50 UTC
//    private long end = 1599034800000L; // 02.09 8:20 UTC
    private long end = 1599220195000L;
    private Map<String, String> ruleNodes = new TreeMap<>();
    {
//        ruleNodes.put("11d8efc0-e60b-11ea-8024-6b507198faf5", "Fetch and Transform");
//        ruleNodes.put("11bd0350-e60b-11ea-8024-6b507198faf5", "Field transformation");
//        ruleNodes.put("11d14ea0-e60b-11ea-8024-6b507198faf5", "Check send Flag");
//        ruleNodes.put("896d7ad0-ece9-11ea-ab7d-11efc3aa32d0", "Ack 'false'");
//        ruleNodes.put("11d74210-e60b-11ea-8024-6b507198faf5", "Ack 'failure'");
//        ruleNodes.put("11c7ffd0-e60b-11ea-8024-6b507198faf5", "Ack 'true'");
        ruleNodes.put("11cfa0f0-e60b-11ea-8024-6b507198faf5", "To TKM Queue");
    }

    @PostConstruct
    public void init(){
        ObjectMapper mapper = new ObjectMapper();
        ruleNodes.forEach((id, name) -> {
            Map<String, AtomicInteger> messagesCounters = new TreeMap<>();
            messagesCounters.put("tb-node-0", new AtomicInteger());
            messagesCounters.put("tb-node-1", new AtomicInteger());
            messagesCounters.put("tb-node-2", new AtomicInteger());
//            AtomicInteger messages = new AtomicInteger();
            template.query("select body from event where event_type = 'STATS' " +
                    "and entity_id = '"+id+"' " +
                    "and ts > "+start+" " +
                    "and ts < "+end+";", (resultSet, i) -> {
                String body = resultSet.getString("body");
                try {
                    Stats stats = mapper.readValue(body, Stats.class);
                    AtomicInteger counter = messagesCounters.get(stats.server);
                    if (counter != null){
                        counter.addAndGet(stats.messagesProcessed);
                    }
//                    messages.addAndGet(stats.messagesProcessed);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return new Object();
            });
//            System.out.println(name + ": " + messages.get());
            System.out.println(name + ": ");
            messagesCounters.forEach((server, messages) -> {
                System.out.println("  " + server + ": " + messages.get());

            });
        });

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Stats {
        public int messagesProcessed;
        public String  server;
        public int  errorsOccurred;
    }
}
