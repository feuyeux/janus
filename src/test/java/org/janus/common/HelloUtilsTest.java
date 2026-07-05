package org.janus.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HelloUtilsTest {

    @Test
    void greetingByValidIndex() {
        assertEquals("Hello", HelloUtils.getGreeting("0"));
        assertEquals("안녕하세요", HelloUtils.getGreeting("5"));
    }

    @Test
    void greetingFallsBackForOutOfRangeOrNonNumeric() {
        assertEquals("Hello", HelloUtils.getGreeting("99"));
        assertEquals("Hello", HelloUtils.getGreeting("-1"));
        assertEquals("Hello", HelloUtils.getGreeting("abc"));
    }

    @Test
    void answerLookup() {
        assertEquals("Thank you very much", HelloUtils.getAnswer("Hello"));
        assertEquals("Merci beaucoup", HelloUtils.getAnswer("Bonjour"));
        assertEquals("Thank you", HelloUtils.getAnswer("unknown-greeting"));
    }
}
