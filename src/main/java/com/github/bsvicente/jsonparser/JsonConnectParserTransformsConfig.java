package com.github.bsvicente.jsonparser;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Set;

@Slf4j
public class JsonConnectParserTransformsConfig extends AbstractConfig {
    public static final String BLACKLIST_CONF = "blacklistFilter";
    public static final String BLACKLIST_DOC = "The field(s) that will be removed from source message.";
    public static final String ENTITY_CONF = "entityName";
    public static final String ENTITY_DOC = "The name of entity that will be parsed.";
    public static final String SORT_CONF = "sortFields";
    public static final String SORT_DOC = "The property that indicates if the elements should be sorted by name. Default it's true";
    public final Set<String> blacklistFilter;
    public final String entityName;
    public final Boolean sortFields;


    public JsonConnectParserTransformsConfig(Map<String, ?> settings) {
        super(config(), settings);
        this.blacklistFilter = Set.copyOf(getList(BLACKLIST_CONF));
        this.entityName = getString(ENTITY_CONF);
        this.sortFields = getBoolean(SORT_CONF);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ENTITY_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, ENTITY_DOC)
                .define(BLACKLIST_CONF, ConfigDef.Type.LIST, "", ConfigDef.Importance.MEDIUM, BLACKLIST_DOC)
                .define(SORT_CONF, ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.LOW, SORT_DOC);
    }

}

