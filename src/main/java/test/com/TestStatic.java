package test.com;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.org.glassfish.external.statistics.Stats;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestStatic {
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        AtomicLong messages = new AtomicLong();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/home/viktor/Temp/file")))) {
            reader.lines().forEach(line -> {
                    Long number = Long.valueOf(line.trim());
                    messages.addAndGet(number);
            });
        } catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(messages.get());
    }
}
