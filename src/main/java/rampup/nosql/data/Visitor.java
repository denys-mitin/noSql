package rampup.nosql.data;

import lombok.Data;

import java.util.UUID;

@Data
public class Visitor {

    private UUID id;

    private String firstName;

    private String lastName;
}
