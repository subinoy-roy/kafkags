package org.roy.kafkags.datasource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Data {
    public static final List<String> UserEmails = List.of(
            "subinoy.roy@hotmail.com",
            "mousumi.sen@gmail.com",
            "tom.manning@gmail.com"
    );

    public static final Map<String, String> UserNames = new HashMap<>(Map.of(
        "subinoy.roy@hotmail.com", "Subinoy Roy",
        "mousumi.sen@gmail.com", "Mousumi Sen",
        "tom.manning@gmail.com", "Tom Manning"
    ));

    public static final List<String> Event = List.of(
            "CLICK",
            "HOVER",
            "BACK",
            "FORWARD",
            "SCROLL",
            "KEY_PRESS",
            "MOVE"
    );
}
