package rampup.nosql.data;

import lombok.Data;

import java.util.UUID;

@Data
public class Visit {

    private UUID id;

    private Station station;

    private Visitor visitor;

    private float amount;

    private float cost;
}
