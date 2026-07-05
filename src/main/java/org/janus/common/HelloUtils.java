package org.janus.common;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HelloUtils {

    private static final List<String> HELLO_LIST =
            Arrays.asList("Hello", "Bonjour", "Hola", "こんにちは", "Ciao", "안녕하세요");

    private static final Map<String, String> ANS_MAP =
            Map.of(
                    "Hello", "Thank you very much",
                    "Bonjour", "Merci beaucoup",
                    "Hola", "Muchas Gracias",
                    "こんにちは", "どうも ありがとう ございます",
                    "Ciao", "Mille Grazie",
                    "안녕하세요", "대단히 감사합니다");

    private HelloUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static String getGreeting(String id) {
        int index;
        try {
            index = Integer.parseInt(id);
        } catch (NumberFormatException e) {
            index = 0;
        }
        if (index < 0 || index >= HELLO_LIST.size()) {
            return "Hello";
        }
        return HELLO_LIST.get(index);
    }

    public static String getAnswer(String greeting) {
        return ANS_MAP.getOrDefault(greeting, "Thank you");
    }
}
