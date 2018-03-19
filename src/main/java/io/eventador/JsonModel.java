package io.eventador;

import java.sql.Timestamp;

public class JsonModel {
    public String field1;
    public String field2;
    public String field3nested1;
    public Timestamp timestamp;

    public JsonModel() {
    }

    public JsonModel(String field1, String field2, String field3nested1, Timestamp timestamp) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3nested1 = field3nested1;
        this.timestamp = timestamp;
    }
}
