import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Solution {

    private enum QueryType {
        AVG_OIL,
        WATER_CUT,
        TOP_WELLS_BY_OIL,
        PRESSURE_DROP
    }

    private static final class Query {
        QueryType type;
        String wellId;
        int k;
        int window;
    }

    private static final class WellAgg {
        double oilSum;
        double waterSum;
        long count;

        void add(double oil, double water) {
            oilSum += oil;
            waterSum += water;
            count++;
        }
    }

    private static final class PendingRecord {
        long timestamp;
        double oil;
        double water;
        double pressure;

        PendingRecord(long timestamp, double oil, double water, double pressure) {
            this.timestamp = timestamp;
            this.oil = oil;
            this.water = water;
            this.pressure = pressure;
        }
    }

    private static final class PressureValue {
        long index;
        double value;

        PressureValue(long index, double value) {
            this.index = index;
            this.value = value;
        }
    }

    private static final class PressureDropTracker {
        final int window;
        long index;
        final ArrayDeque<PressureValue> minDeque;
        final ArrayDeque<PressureValue> maxDeque;
        boolean hasBest;
        double bestDrop;

        PressureDropTracker(int window) {
            this.window = window;
            this.index = 0L;
            this.minDeque = new ArrayDeque<>();
            this.maxDeque = new ArrayDeque<>();
            this.hasBest = false;
            this.bestDrop = 0.0;
        }

        void add(double pressure) {
            while (!minDeque.isEmpty() && minDeque.peekLast().value >= pressure) {
                minDeque.removeLast();
            }
            minDeque.addLast(new PressureValue(index, pressure));

            while (!maxDeque.isEmpty() && maxDeque.peekLast().value <= pressure) {
                maxDeque.removeLast();
            }
            maxDeque.addLast(new PressureValue(index, pressure));

            long windowStart = index - window + 1L;
            while (!minDeque.isEmpty() && minDeque.peekFirst().index < windowStart) {
                minDeque.removeFirst();
            }
            while (!maxDeque.isEmpty() && maxDeque.peekFirst().index < windowStart) {
                maxDeque.removeFirst();
            }

            if (index + 1L >= window) {
                double currentDrop = maxDeque.peekFirst().value - minDeque.peekFirst().value;
                if (!hasBest || currentDrop > bestDrop) {
                    bestDrop = currentDrop;
                    hasBest = true;
                }
            }

            index++;
        }
    }

    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

        List<Query> queries = new ArrayList<>();
        Map<String, WellAgg> aggregates = new HashMap<>();
        Map<String, PendingRecord> pendingByWell = new HashMap<>();
        Map<String, List<PressureDropTracker>> pressureTrackersByWell = new HashMap<>();

        boolean inQueries = false;
        boolean inData = false;

        String line;
        while ((line = reader.readLine()) != null) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            if ("QUERIES".equals(trimmed)) {
                inQueries = true;
                inData = false;
                continue;
            }
            if ("DATA".equals(trimmed)) {
                inQueries = false;
                inData = true;
                continue;
            }
            if ("END".equals(trimmed)) {
                break;
            }

            if (inQueries) {
                Query q = parseQuery(trimmed, pressureTrackersByWell);
                if (q != null) {
                    queries.add(q);
                }
                continue;
            }

            if (inData) {
                processDataLine(line, pendingByWell, aggregates, pressureTrackersByWell);
            }
        }

        flushAllPending(pendingByWell, aggregates, pressureTrackersByWell);

        List<String> outputs = new ArrayList<>(queries.size());
        for (Query q : queries) {
            outputs.add(answerQuery(q, aggregates, pressureTrackersByWell));
        }

        StringBuilder out = new StringBuilder();
        for (int i = 0; i < outputs.size(); i++) {
            out.append(outputs.get(i));
            if (i + 1 < outputs.size()) {
                out.append('\n');
            }
        }
        System.out.print(out);
    }

    private static Query parseQuery(String line, Map<String, List<PressureDropTracker>> pressureTrackersByWell) {
        if (line.startsWith("AVG_OIL ")) {
            String wellId = extractParamValue(line, "well_id=");
            if (wellId == null) {
                return null;
            }
            Query q = new Query();
            q.type = QueryType.AVG_OIL;
            q.wellId = wellId;
            return q;
        }

        if (line.startsWith("WATER_CUT ")) {
            String wellId = extractParamValue(line, "well_id=");
            if (wellId == null) {
                return null;
            }
            Query q = new Query();
            q.type = QueryType.WATER_CUT;
            q.wellId = wellId;
            return q;
        }

        if (line.startsWith("TOP_WELLS_BY_OIL ")) {
            String kValue = extractParamValue(line, "k=");
            if (kValue == null) {
                return null;
            }
            int k;
            try {
                k = Integer.parseInt(kValue);
            } catch (NumberFormatException e) {
                return null;
            }
            Query q = new Query();
            q.type = QueryType.TOP_WELLS_BY_OIL;
            q.k = Math.max(0, k);
            return q;
        }

        if (line.startsWith("PRESSURE_DROP ")) {
            String wellId = extractParamValue(line, "well_id=");
            String windowValue = extractParamValue(line, "window=");
            if (wellId == null || windowValue == null) {
                return null;
            }

            int window;
            try {
                window = Integer.parseInt(windowValue);
            } catch (NumberFormatException e) {
                return null;
            }

            Query q = new Query();
            q.type = QueryType.PRESSURE_DROP;
            q.wellId = wellId;
            q.window = window;

            if (window > 0) {
                pressureTrackersByWell
                        .computeIfAbsent(wellId, ignored -> new ArrayList<>())
                        .add(new PressureDropTracker(window));
            }
            return q;
        }

        return null;
    }

    private static String extractParamValue(String line, String key) {
        int idx = line.indexOf(key);
        if (idx < 0) {
            return null;
        }
        int start = idx + key.length();
        int end = start;
        while (end < line.length() && !Character.isWhitespace(line.charAt(end))) {
            end++;
        }
        if (start == end) {
            return null;
        }
        return line.substring(start, end);
    }

    private static void processDataLine(
            String line,
            Map<String, PendingRecord> pendingByWell,
            Map<String, WellAgg> aggregates,
            Map<String, List<PressureDropTracker>> pressureTrackersByWell
    ) {
        String[] parts = line.split(",", -1);
        if (parts.length != 5) {
            return;
        }

        String wellId = parts[0];
        if (wellId.isEmpty()) {
            return;
        }

        long timestamp;
        double oil;
        double water;
        double pressure;

        try {
            timestamp = Long.parseLong(parts[1]);
            oil = Double.parseDouble(parts[2]);
            water = Double.parseDouble(parts[3]);
            pressure = Double.parseDouble(parts[4]);
        } catch (NumberFormatException e) {
            return;
        }

        if (oil < 0.0 || water < 0.0) {
            return;
        }

        PendingRecord pending = pendingByWell.get(wellId);
        if (pending == null) {
            pendingByWell.put(wellId, new PendingRecord(timestamp, oil, water, pressure));
            return;
        }

        if (timestamp == pending.timestamp) {
            pending.oil = oil;
            pending.water = water;
            pending.pressure = pressure;
            return;
        }

        flushOne(wellId, pending, aggregates, pressureTrackersByWell);
        pendingByWell.put(wellId, new PendingRecord(timestamp, oil, water, pressure));
    }

    private static void flushOne(
            String wellId,
            PendingRecord pending,
            Map<String, WellAgg> aggregates,
            Map<String, List<PressureDropTracker>> pressureTrackersByWell
    ) {
        WellAgg agg = aggregates.computeIfAbsent(wellId, ignored -> new WellAgg());
        agg.add(pending.oil, pending.water);

        List<PressureDropTracker> trackers = pressureTrackersByWell.get(wellId);
        if (trackers == null) {
            return;
        }
        for (PressureDropTracker tracker : trackers) {
            tracker.add(pending.pressure);
        }
    }

    private static void flushAllPending(
            Map<String, PendingRecord> pendingByWell,
            Map<String, WellAgg> aggregates,
            Map<String, List<PressureDropTracker>> pressureTrackersByWell
    ) {
        for (Map.Entry<String, PendingRecord> entry : pendingByWell.entrySet()) {
            flushOne(entry.getKey(), entry.getValue(), aggregates, pressureTrackersByWell);
        }
    }

    private static String answerQuery(
            Query q,
            Map<String, WellAgg> aggregates,
            Map<String, List<PressureDropTracker>> pressureTrackersByWell
    ) {
        if (q.type == QueryType.AVG_OIL) {
            WellAgg agg = aggregates.get(q.wellId);
            if (agg == null || agg.count == 0) {
                return "NA";
            }
            return format6(agg.oilSum / agg.count);
        }

        if (q.type == QueryType.WATER_CUT) {
            WellAgg agg = aggregates.get(q.wellId);
            if (agg == null) {
                return "NA";
            }
            double liquid = agg.oilSum + agg.waterSum;
            if (liquid == 0.0) {
                return "0.000000";
            }
            return format6(agg.waterSum / liquid);
        }

        if (q.type == QueryType.PRESSURE_DROP) {
            if (q.window <= 0) {
                return "NA";
            }

            List<PressureDropTracker> trackers = pressureTrackersByWell.get(q.wellId);
            if (trackers == null || trackers.isEmpty()) {
                return "NA";
            }

            for (PressureDropTracker tracker : trackers) {
                if (tracker.window == q.window) {
                    if (!tracker.hasBest) {
                        return "NA";
                    }
                    return format6(tracker.bestDrop);
                }
            }
            return "NA";
        }

        List<Map.Entry<String, WellAgg>> wells = new ArrayList<>(aggregates.entrySet());
        wells.sort(
                Comparator
                        .<Map.Entry<String, WellAgg>>comparingDouble(e -> e.getValue().oilSum)
                        .reversed()
                        .thenComparing(Map.Entry::getKey)
        );

        int limit = Math.min(q.k, wells.size());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < limit; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(wells.get(i).getKey());
        }
        return sb.toString();
    }

    private static String format6(double v) {
        return String.format(Locale.US, "%.6f", v);
    }
}