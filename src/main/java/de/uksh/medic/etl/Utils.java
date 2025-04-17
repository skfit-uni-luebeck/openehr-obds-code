package de.uksh.medic.etl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public final class Utils {

    private Utils() {
    }

    public static void listConv(Map<String, Object> input) {
        input.entrySet().forEach(e -> {
            if (e.getValue() == null || e.getValue() instanceof List) {
                return;
            }
            List<Object> l = new ArrayList<>();
            l.add(e.getValue());
            e.setValue(l);
        });
    }

    public static Map<String, Object> formatMap(Map<String, Object> input) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Entry<String, Object> e : input.entrySet()) {
            ArrayList<String> al = new ArrayList<>(Arrays.asList(e.getKey().split("/")));
            splitMap(e.getValue(), al, out);
        }
        return out;
    }

    @SuppressWarnings({ "unchecked" })
    private static void splitMap(Object value, List<String> key, Map<String, Object> out) {
        if (key.size() > 1) {
            String k = key.removeFirst();
            if (!(out.get(k) instanceof List)) {
                Map<String, Object> m = (Map<String, Object>) out.getOrDefault(k,
                        new LinkedHashMap<>());
                out.put(k, m);
                splitMap(value, key, m);

            } else {
                List<Map<String, Object>> l = (List<Map<String, Object>>) out.get(k);
                Map<String, Object> m = l.get(0);
                out.put(k, m);
                splitMap(value, key, m);
            }
        } else if (key.size() == 1 && !out.containsKey(key.getFirst())) {
            out.put(key.removeFirst(), value);
        } else if (key.size() == 1 && out.containsKey(key.getFirst())) {
            if (value instanceof List && out.get(key.getFirst()) instanceof List) {
                ((List<Object>) out.get(key.getFirst())).addAll((List<Object>) value);
            }
            if (value instanceof List && out.get(key.getFirst()) instanceof Map) {
                ((List<Map<String, Object>>) value)
                        .forEach(m -> m.putAll((Map<String, Object>) out.get(key.getFirst())));
                out.put(key.getFirst(), value);
            }
            if (value instanceof Map && out.get(key.getFirst()) instanceof Map) {
                ((Map<String, Object>) out.get(key.getFirst())).putAll((Map<String, Object>) value);
            }
            if (value instanceof Map && out.get(key.getFirst()) instanceof List) {
                ((List<Map<String, Object>>) out.get(key.getFirst()))
                        .forEach(l -> l.putAll((Map<String, Object>) value));
                out.put(key.getFirst(), value);
            }
        }
    }

}
