package shared.messages;

import java.io.Serializable;

public class Message implements KVMessage, Serializable {

    private String key;
    private String val;
    private StatusType status;

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return val;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }
}