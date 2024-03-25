package shared.messages;

import java.io.Serializable;

public class TextMessage implements Serializable {

    private static final long serialVersionUID = 6635351644223964688L;
    private String textMessage;
    private byte[] byteMessage;
    private static final char NEWLINE = 0x0A;
    private static final char RETURN = 0x0D;

    public TextMessage(String msg) {
        textMessage = msg;
        byteMessage = byteArray(msg);
    }

    public TextMessage(byte[] msg) {
        textMessage = new String(msg).trim();
        byteMessage = delimiter(msg);
    }

    public String getTextMessage() {
        return textMessage;
    }

    public byte[] getByteMessage() {
        return byteMessage;
    }

    private byte[] byteArray(String msg) {
        byte[] byteMsg = msg.getBytes();
        byte[] delimiter = new byte[]{NEWLINE, RETURN};
        byte[] byteMessage = new byte[byteMsg.length + delimiter.length];
        System.arraycopy(byteMsg, 0, byteMessage, 0, byteMsg.length);
        System.arraycopy(delimiter, 0, byteMessage, byteMsg.length, delimiter.length);
        return byteMessage;
    }

    private byte[] delimiter(byte[] msg) {
        byte[] delimiter = new byte[]{NEWLINE, RETURN};
        byte[] byteMessage = new byte[msg.length + delimiter.length];
        System.arraycopy(msg, 0, byteMessage, 0, msg.length);
        System.arraycopy(delimiter, 0, byteMessage, msg.length, delimiter.length);
        return byteMessage;
    }
}