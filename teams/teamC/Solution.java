import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Solution {

    static class DataRecord {
        double oil;
        double water;
        double pressure;

        DataRecord(double oil, double water, double pressure) {
            this.oil = oil;
            this.water = water;
            this.pressure = pressure;
        }
    }

    static class WellData {
        double sumOil = 0;
        double sumWater = 0;
        int count = 0;
        Map<Long, DataRecord> timestampData = new LinkedHashMap<>();
        List<Double> pressureList = new ArrayList<>();

        void addRecord(long timestamp, double oil, double water, double pressure) {
            DataRecord existing = timestampData.get(timestamp);
            if (existing != null) {
                // Заменяем дубликат - берем последний
                sumOil -= existing.oil;
                sumWater -= existing.water;
                existing.oil = oil;
                existing.water = water;
                existing.pressure = pressure;
                sumOil += oil;
                sumWater += water;
            } else {
                // Новая запись
                timestampData.put(timestamp, new DataRecord(oil, water, pressure));
                sumOil += oil;
                sumWater += water;
                count++;
            }
        }

        void finalizePressureList() {
            pressureList.clear();
            for (DataRecord record : timestampData.values()) {
                pressureList.add(record.pressure);
            }
        }

        double getAvgOil() {
            return count > 0 ? sumOil / count : 0;
        }

        double getWaterCut() {
            double totalLiquid = sumOil + sumWater;
            return totalLiquid > 0 ? sumWater / totalLiquid : 0.0;
        }

        double getMaxPressureDrop(int windowSize) {
            if (windowSize <= 0 || pressureList.size() < windowSize) {
                return Double.NaN;
            }

            double maxDrop = 0.0;

            for (int i = 0; i <= pressureList.size() - windowSize; i++) {
                double min = Double.MAX_VALUE;
                double max = Double.MIN_VALUE;

                for (int j = i; j < i + windowSize; j++) {
                    double p = pressureList.get(j);
                    if (p < min) min = p;
                    if (p > max) max = p;
                }

                double drop = max - min;
                if (drop > maxDrop) {
                    maxDrop = drop;
                }
            }

            return maxDrop;
        }
    }


    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        List<String> queries = new ArrayList<>();
        Map<String, WellData> wells = new HashMap<>();

        String line;
        boolean inQueries = false;
        boolean inData = false;

        while ((line = reader.readLine()) != null) {
            line = line.trim();

            if (line.isEmpty()) {
                continue;
            }

            if (line.equals("QUERIES")) {
                inQueries = true;
                continue;
            }

            if (line.equals("DATA")) {
                inQueries = false;
                inData = true;
                continue;
            }

            if (line.equals("END")) {
                break;
            }

            if (inQueries) {
                queries.add(line);
            } else if (inData) {
                processDataLine(line, wells);
            }
        }

        // Финализация данных по давлению
        for (WellData wellData : wells.values()) {
            wellData.finalizePressureList();
        }

        // Обработка запросов
        for (String query : queries) {
            String result = processQuery(query, wells);
            System.out.println(result);
        }
    }

    static void processDataLine(String line, Map<String, WellData> wells) {
        String[] parts = line.split(",", -1);

        // Проверка количества полей
        if (parts.length != 5) {
            return;
        }

        try {
            String wellId = parts[0];
            if (wellId.isEmpty()) {
                return;
            }

            long timestamp = Long.parseLong(parts[1]);
            double oilRate = Double.parseDouble(parts[2]);
            double waterRate = Double.parseDouble(parts[3]);
            double pressure = Double.parseDouble(parts[4]);

            // Проверка на отрицательные значения
            if (oilRate < 0 || waterRate < 0) {
                return;
            }

            // Добавляем или обновляем запись
            wells.computeIfAbsent(wellId, k -> new WellData())
                 .addRecord(timestamp, oilRate, waterRate, pressure);

        } catch (NumberFormatException e) {
            // Игнорируем невалидные строки
        }
    }

    static String processQuery(String query, Map<String, WellData> wells) {
        if (query.startsWith("AVG_OIL")) {
            String wellId = extractParameter(query, "well_id");
            WellData data = wells.get(wellId);
            if (data == null || data.count == 0) {
                return "NA";
            }
            return String.format(Locale.US, "%.6f", data.getAvgOil());

        } else if (query.startsWith("WATER_CUT")) {
            String wellId = extractParameter(query, "well_id");
            WellData data = wells.get(wellId);
            if (data == null || data.count == 0) {
                return "NA";
            }
            return String.format(Locale.US, "%.6f", data.getWaterCut());

        } else if (query.startsWith("TOP_WELLS_BY_OIL")) {
            int k = Integer.parseInt(extractParameter(query, "k"));
            return getTopWells(wells, k);

        } else if (query.startsWith("PRESSURE_DROP")) {
            String wellId = extractParameter(query, "well_id");
            int window = Integer.parseInt(extractParameter(query, "window"));

            if (window <= 0) {
                return "NA";
            }

            WellData data = wells.get(wellId);
            if (data == null || data.count == 0) {
                return "NA";
            }

            double maxDrop = data.getMaxPressureDrop(window);
            if (Double.isNaN(maxDrop)) {
                return "NA";
            }

            return String.format(Locale.US, "%.6f", maxDrop);
        }

        return "";
    }

    static String extractParameter(String query, String paramName) {
        String prefix = paramName + "=";
        int idx = query.indexOf(prefix);
        if (idx == -1) {
            return "";
        }
        int start = idx + prefix.length();
        int end = query.indexOf(' ', start);
        if (end == -1) {
            end = query.length();
        }
        return query.substring(start, end);
    }

    static String getTopWells(Map<String, WellData> wells, int k) {
        List<Map.Entry<String, WellData>> entries = new ArrayList<>(wells.entrySet());

        // Сортировка: по убыванию sumOil, при равенстве по возрастанию well_id
        entries.sort((e1, e2) -> {
            int cmp = Double.compare(e2.getValue().sumOil, e1.getValue().sumOil);
            if (cmp != 0) {
                return cmp;
            }
            return e1.getKey().compareTo(e2.getKey());
        });

        StringBuilder result = new StringBuilder();
        int count = Math.min(k, entries.size());
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                result.append(",");
            }
            result.append(entries.get(i).getKey());
        }

        return result.toString();
    }
}