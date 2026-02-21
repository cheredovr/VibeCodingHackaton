import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * BigInputGen
 *
 * Generates a LARGE input for Production Log Aggregator and writes it to stdout.
 * IMPORTANT: This is ONLY a generator (no expected output, no oracle logic).
 *
 * Format produced:
 *   QUERIES
 *   <queries...>
 *   DATA
 *   <data lines...>      // timestamp sorted ascending
 *   END
 *
 * Goals:
 * - Let teams stress-test I/O + streaming + memory behavior on up to 1e7 DATA lines.
 * - Include duplicates within the same timestamp block (same well_id,timestamp) to stress "last wins"
 *   without revealing any checking logic.
 *
 * Usage:
 *   javac BigInputGen.java
 *
 *   # Default: 1e7 lines
 *   java BigInputGen > big.in
 *
 *   # Pipe directly into a solution:
 *   java BigInputGen | java -cp ./teams/teamA Solution > /dev/null
 *
 * Options:
 *   --lines N    total DATA lines (default 10_000_000)
 *   --wells N    number of distinct wells (default 50)
 *   --block N    base unique-well lines per timestamp (default min(wells, 200))
 *   --dups N     duplicates per timestamp block (default 3)
 *   --seed N     RNG seed (default 1)
 *
 * Notes:
 * - Well IDs: W1..W{wells}
 * - Values:
 *   oil_rate   >= 0
 *   water_rate >= 0
 *   pressure   arbitrary positive-ish
 * - Numbers are printed with 3 decimals (Locale.ROOT).
 */
public class BigInputGen {

    static final long DEFAULT_LINES = 10_000_000L;
    static final int  DEFAULT_WELLS = 50;
    static final int  DEFAULT_BLOCK = 200;
    static final int  DEFAULT_DUPS  = 3;
    static final long DEFAULT_SEED  = 1L;

    public static void main(String[] args) throws Exception {
        long lines = DEFAULT_LINES;
        int wells = DEFAULT_WELLS;
        int block = DEFAULT_BLOCK;
        int dups = DEFAULT_DUPS;
        long seed = DEFAULT_SEED;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--lines" -> lines = Long.parseLong(args[++i]);
                case "--wells" -> wells = Integer.parseInt(args[++i]);
                case "--block" -> block = Integer.parseInt(args[++i]);
                case "--dups"  -> dups = Integer.parseInt(args[++i]);
                case "--seed"  -> seed = Long.parseLong(args[++i]);
                default -> {
                    System.err.println("Unknown arg: " + args[i]);
                    System.err.println("Supported: --lines N --wells N --block N --dups N --seed N");
                    System.exit(2);
                }
            }
        }

        if (wells <= 0) throw new IllegalArgumentException("--wells must be > 0");
        block = Math.max(1, Math.min(block, wells));
        dups = Math.max(0, dups);
        if (lines < 0) throw new IllegalArgumentException("--lines must be >= 0");

        SplittableRandom rng = new SplittableRandom(seed);

        // Prebuild well IDs for speed
        String[] wellIds = new String[wells];
        for (int i = 0; i < wells; i++) wellIds[i] = "W" + (i + 1);

        try (PrintWriter out = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), 1 << 20),
                false
        )) {
            // QUERIES (fixed set, can be anything; it’s for stress only)
            out.println("QUERIES");
            out.println("TOP_WELLS_BY_OIL k=10");
            out.println("AVG_OIL well_id=W1");
            out.println("WATER_CUT well_id=W1");
            out.println("AVG_OIL well_id=W2");
            out.println("WATER_CUT well_id=W2");
            out.println("PRESSURE_DROP well_id=W1 window=32");
            out.println("PRESSURE_DROP well_id=W2 window=64");
            out.println("PRESSURE_DROP well_id=W3 window=128");
            out.println("DATA");

            // DATA
            long emitted = 0L;
            long tsBase = 1_700_000_000L;

            // Roughly (block + dups) lines per timestamp block
            long approxPerTs = (long) block + (long) dups;
            long blocks = Math.max(1L, (lines + approxPerTs - 1) / approxPerTs);

            int[] used = new int[block];

            for (long b = 0; b < blocks && emitted < lines; b++) {
                long ts = tsBase + b; // sorted ascending by construction

                int start = (int) (b % wells);
                int usedN = 0;

                // Base unique wells
                for (int j = 0; j < block && emitted < lines; j++) {
                    int idx = (start + j) % wells;
                    used[usedN++] = idx;

                    double oil = 10.0 + (idx * 0.1) + rng.nextDouble(0.0, 5.0);
                    double water = rng.nextDouble(0.0, 15.0);
                    double pressure = 100.0 + rng.nextDouble(-10.0, 10.0) + (idx * 0.05);

                    writeLine(out, wellIds[idx], ts, oil, water, pressure);
                    emitted++;
                }

                // Duplicates within same timestamp (last wins stress)
                for (int d = 0; d < dups && emitted < lines; d++) {
                    int pick = used[rng.nextInt(Math.max(1, usedN))];

                    // Make duplicates visibly different
                    double oil = 50.0 + rng.nextDouble(0.0, 10.0);
                    double water = rng.nextDouble(0.0, 5.0);
                    double pressure = 150.0 + rng.nextDouble(-3.0, 3.0);

                    writeLine(out, wellIds[pick], ts, oil, water, pressure);
                    emitted++;
                }

                // Flush periodically so pipelines don't buffer too much
                if ((b & 0x3FF) == 0) out.flush();
            }

            out.println("END");
            out.flush();
        }
    }

    private static void writeLine(PrintWriter out, String well, long ts, double oil, double water, double pressure) {
        out.print(well);
        out.print(',');
        out.print(ts);
        out.print(',');
        out.print(fmt3(oil));
        out.print(',');
        out.print(fmt3(water));
        out.print(',');
        out.println(fmt3(pressure));
    }

    private static String fmt3(double x) {
        return String.format(Locale.ROOT, "%.3f", x);
    }
}