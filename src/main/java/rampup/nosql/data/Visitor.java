package rampup.nosql.data;

import com.google.gson.annotations.JsonAdapter;
import lombok.Data;
import rampup.nosql.mapping.UuidJsonAdapter;

import java.util.UUID;

@Data
public class Visitor {

    @JsonAdapter(UuidJsonAdapter.class)
    private UUID id;

    private String firstName;

    private String lastName;
}
