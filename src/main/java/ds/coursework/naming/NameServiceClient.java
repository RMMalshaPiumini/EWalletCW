package ds.coursework.naming;

import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class NameServiceClient {
    private String etcdAddress;

    public NameServiceClient(String etcdAddress) {
        this.etcdAddress = etcdAddress;
    }

    // Register a service (e.g., "Shard-1", "127.0.0.1", 50051)
    public void registerService(String serviceName, String ip, int port) throws IOException {
        String key = serviceName;
        String value = new JSONObject().put("ip", ip).put("port", String.valueOf(port)).toString();

        String payload = new JSONObject()
                .put("key", Base64.getEncoder().encodeToString(key.getBytes()))
                .put("value", Base64.getEncoder().encodeToString(value.getBytes()))
                .toString();
        callEtcd(etcdAddress + "/v3/kv/put", payload);
    }

    public ServiceDetails findService(String serviceName) throws IOException {
        String payload = new JSONObject()
                .put("key", Base64.getEncoder().encodeToString(serviceName.getBytes()))
                .toString();
        String response = callEtcd(etcdAddress + "/v3/kv/range", payload);

        JSONObject json = new JSONObject(response);
        if (!json.has("kvs")) return null;

        String encodedValue = json.getJSONArray("kvs").getJSONObject(0).getString("value");
        String decodedValue = new String(Base64.getDecoder().decode(encodedValue));
        JSONObject details = new JSONObject(decodedValue);

        return new ServiceDetails(details.getString("ip"), Integer.parseInt(details.getString("port")));
    }

    private String callEtcd(String urlStr, String payload) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload.getBytes(StandardCharsets.UTF_8));
        }
        try (InputStream is = conn.getInputStream()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    public static class ServiceDetails {
        public String ip;
        public int port;
        public ServiceDetails(String ip, int port) { this.ip = ip; this.port = port; }
    }
}