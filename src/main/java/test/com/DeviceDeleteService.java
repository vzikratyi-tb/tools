package test.com;

import org.springframework.stereotype.Service;
import org.thingsboard.rest.client.RestClient;
import org.thingsboard.server.common.data.id.DeviceId;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.UUID;

@Service
public class DeviceDeleteService {

    public void deleteDevices(RestClient restClient) throws Exception{
        try (BufferedReader reader = new BufferedReader(new FileReader(new File("/home/viktor/Work/tools/conf/T_devices_to_delete.txt")))) {
            reader.lines().forEach(deviceId -> {
                try {
                    restClient.deleteDevice(new DeviceId(UUID.fromString(deviceId)));
                } catch (Exception e){
                    System.out.println("Failed to delete device " + deviceId);
                    e.printStackTrace();
                }
            });
        }
    }
}
