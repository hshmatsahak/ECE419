package shared.messages;

import java.io.Serializable;

public class Message implements KVMessage, Serializable {

    private static final long serialVersionUID = 8125878201977457676L;
    private StatusType type;
    private String key;
    private String val;

    public Message(StatusType t, String k, String v) {
        type = t;
        key = k;
        val = v;
    }

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
        return type;
    }
}