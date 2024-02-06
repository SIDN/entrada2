package nl.sidn.entrada2.messaging;

import lombok.Data;

@Data
public class EventMessage {
    private String eventName;
    private String key;
}