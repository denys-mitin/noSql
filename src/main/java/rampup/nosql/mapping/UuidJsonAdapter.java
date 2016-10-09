package rampup.nosql.mapping;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.UUID;

public class UuidJsonAdapter extends TypeAdapter<UUID> {

    @Override
    public void write(JsonWriter jsonWriter, UUID uuid) throws IOException {
        jsonWriter.value(uuid.toString().replace("-", ""));
    }

    @Override
    public UUID read(JsonReader jsonReader) throws IOException {
        return UUID.fromString(jsonReader.nextString().
                replaceAll("(\\w{8})(\\w{4})(\\w{4})(\\w{4})(\\w{12})", "$1-$2-$3-$4-$5"));
    }
}
