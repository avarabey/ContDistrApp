package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.config.RefDataProperties;
import com.contdistrapp.refdata.error.BadRequestException;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Map;

@Component
public class DictionaryRegistry {

    private final Map<String, RefDataProperties.Dictionary> dictionaries;

    public DictionaryRegistry(RefDataProperties properties) {
        this.dictionaries = properties.dictionaryMap();
    }

    public RefDataProperties.Dictionary required(String dictCode) {
        if (dictCode == null) {
            throw new BadRequestException("dictCode is required");
        }
        RefDataProperties.Dictionary cfg = dictionaries.get(dictCode.toUpperCase(Locale.ROOT));
        if (cfg == null) {
            throw new BadRequestException("Dictionary is not configured: " + dictCode);
        }
        return cfg;
    }
}
