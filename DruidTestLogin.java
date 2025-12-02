import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class DruidTestLogin {

    // SHA256 helper
    public static String sha256(String input) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if(hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public static void main(String[] args) throws Exception {

        String username = "simeon.lisovski@awi.de";
        String password = "PolarConnection";  // your REAL password

        // Hash format EXACTLY from the official druid manual
        String raw = username + "druid" + password + "heifeng";
        String hashed = sha256(raw);

        String json = String.format(
            "{\"username\":\"%s\",\"password\":\"%s\"}",
            username, hashed
        );

        System.out.println("LOGIN JSON SENT:");
        System.out.println(json);

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("https://www.ecotopiago.com/api/v2/login"))
            .header("Content-Type", "application/json;charset=UTF-8")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();

        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println("STATUS: " + response.statusCode());
        System.out.println("BODY: " + response.body());
        System.out.println("HEADERS: " + response.headers());
    }
}