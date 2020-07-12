package com.tnpacs.dicoogle.mongoplugin.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DictionaryTest {
    Dictionary dict = Dictionary.getInstance();
    ClassLoader classLoader = getClass().getClassLoader();

    @Test
    void testToString() {
        System.out.println(dict);
    }
}