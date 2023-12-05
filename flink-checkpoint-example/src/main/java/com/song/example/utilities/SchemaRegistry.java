package com.song.example.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.avro.Schema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.json.JSONArray;
import org.json.JSONObject;

public class SchemaRegistry {

    public static Schema LoadSchema(ParameterTool parameter) throws IOException {

        String schemaRegistryUrl = parameter.get("schema-registry");
        String topic = parameter.get("topic");

        String schemaName = topic + "-value";

        int version = -1;
        {
            URL url = new URL(schemaRegistryUrl + "/subjects/" + schemaName + "/versions");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            BufferedReader response = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String txt = response.readLine();
            conn.disconnect();

            JSONArray versions = new JSONArray(txt);
            for (int i = 0; i < versions.length(); i++) {
                int temp = versions.getInt(i);
                version = (temp > version) ? temp : version;
            }
        }

        String schema_text = null;
        {
            URL url = new URL(schemaRegistryUrl + "/subjects/" + schemaName + "/versions/" + version);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            BufferedReader response = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String txt = response.readLine();
            conn.disconnect();

            JSONObject obj = new JSONObject(txt);
            schema_text = obj.getString("schema");
        }

        var schema = new Schema.Parser().parse(schema_text);
        return schema;
    }
}
