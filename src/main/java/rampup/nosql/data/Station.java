package rampup.nosql.data;

import com.google.gson.annotations.JsonAdapter;
import lombok.Data;
import org.elasticsearch.common.geo.GeoPoint;
import rampup.nosql.mapping.UuidJsonAdapter;

import java.util.UUID;

@Data
public class Station {

    @JsonAdapter(UuidJsonAdapter.class)
    private UUID id;

    private String name;

    private GeoPoint location;
}
