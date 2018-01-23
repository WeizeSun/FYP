package fyp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

public class Monitor {

    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    public static JSONObject readJsonFromUrl(String url) 
        throws IOException, JSONException {
        InputStream is = new URL(url).openStream();
        try {
            BufferedReader rd 
                = new BufferedReader(
                        new InputStreamReader(
                            is,
                            Charset.forName("UTF-8")));
            String jsonText = readAll(rd);
            JSONObject json = new JSONObject(jsonText);
            return json;
        } finally {
            is.close();
        }
    }

    public static void main(String[] args) 
        throws IOException, JSONException {
        while (true) {
            JSONObject json 
                = readJsonFromUrl("http://localhost:8081/jobs/" + args[0]);
            
            System.out.println("======================================");
            String state = json.getString("state");
            if (state.equals("FINISHED")) {
                System.out.println("The job is finished, "
                        + "thus the monitor termintes automatically!");
                break;
            }

            JSONArray vertices = (JSONArray) json.get("vertices");
            JSONObject kmean = vertices.getJSONObject(2);
            long duration = kmean.getLong("duration");
            JSONObject metrics = kmean.getJSONObject("metrics");
            long readBytes = metrics.getLong("read-bytes");
            long writeBytes = metrics.getLong("write-bytes");

            System.out.println("Now the operator has been running for " 
                    + duration);
            System.out.println("Now the operator has read " 
                    + readBytes
                    + " bytes");
            System.out.println("Now the operator has read " 
                    + writeBytes
                    + " bytes");

            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                System.out.println("The Monitor is interrupted!");
                return;
            }
        }
    }
}
