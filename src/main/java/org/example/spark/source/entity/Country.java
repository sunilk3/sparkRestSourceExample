package org.example.spark.source.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Country {
    public Name name;
    public List<String> tld;
    public String cca2;
    public String ccn3;
    public String cioc;
    public boolean independent;
    public String status;

    @JsonProperty("unMember")
    public boolean unMember;

    public Map<String, Currency> currencies;
    public Idd idd;
    public List<String> capital;
    public List<String> altSpellings;
    public String region;
    public String subregion;
    public Map<String, String> languages;
    public List<Double> latlng;
    public boolean landlocked;
    public List<String> borders;
    public double area;
    public Map<String, Demonym> demonyms;
    public String cca3;
    public Map<String, Translation> translations;
    public String flag;
    public Maps maps;
    public long population;
    public Map<String, Double> gini;
    public String fifa;
    public Car car;
    public List<String> timezones;
    public List<String> continents;
    public Flags flags;
    public CoatOfArms coatOfArms;
    public String startOfWeek;
    public CapitalInfo capitalInfo;
    public PostalCode postalCode;

    // --- Inner Classes ---

    public static class Name {
        public String common;
        public String official;
        public Map<String, Translation> nativeName;
    }

    public static class Translation {
        public String official;
        public String common;
    }

    public static class Currency {
        public String name;
        public String symbol;
    }

    public static class Idd {
        public String root;
        public List<String> suffixes;
    }

    public static class Demonym {
        public String f;
        public String m;
    }

    public static class Maps {
        public String googleMaps;
        public String openStreetMaps;
    }

    public static class Car {
        public List<String> signs;
        public String side;
    }

    public static class Flags {
        public String png;
        public String svg;
        public String alt;
    }

    public static class CoatOfArms {
        public String png;
        public String svg;
    }

    public static class CapitalInfo {
        public List<Double> latlng;
    }

    public static class PostalCode {
        public String format;
        public String regex;
    }
}