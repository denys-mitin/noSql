package rampup.nosql.data;

import lombok.Data;

import java.util.UUID;

@Data
public class Station {

    private UUID id;

    private String name;
}
