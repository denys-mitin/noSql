package rampup.nosql.data;

import lombok.Data;

@Data
public class Visit {

    private Station station;

    private Visitor visitor;

    private float amount;

    private float cost;

    private long time;
}
