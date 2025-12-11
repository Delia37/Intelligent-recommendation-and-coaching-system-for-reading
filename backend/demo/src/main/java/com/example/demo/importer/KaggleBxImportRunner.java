package com.example.demo.importer;

import com.example.demo.catalog.Book;
import com.example.demo.catalog.BookRepository;
import com.example.demo.catalog.Rating;
import com.example.demo.catalog.RatingRepository;
import com.example.demo.identity.AppUser;
import com.example.demo.identity.AppUserRepository;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name="app.import.enabled", havingValue="true")
public class KaggleBxImportRunner implements CommandLineRunner {

    private static final Charset CSV_CHARSET = StandardCharsets.ISO_8859_1; // BX dataset charset

    private final BookRepository books;
    private final AppUserRepository users;
    private final RatingRepository ratings;

    @PersistenceContext private EntityManager em;

    @Value("${app.data-dir}")
    private String dataDir;

    @Override
    @Transactional
    public void run(String... args) throws Exception {
        if (books.count() > 0 || users.count() > 0) {
            System.out.println("Import skipped: data already present.");
            return;
        }

        Path booksCsv   = resolveOne("BX-Books.csv", "Books.csv", "books.csv");
        Path usersCsv   = resolveOne("BX-Users.csv", "Users.csv", "users.csv");
        Path ratingsCsv = resolveOne("BX-Book-Ratings.csv", "Ratings.csv", "ratings.csv");

        Map<String, Long> isbnToId    = importBooks(booksCsv);
        Map<String, Long> extUserToId = importUsers(usersCsv);
        importRatings(ratingsCsv, isbnToId, extUserToId);

        System.out.printf("Import done: books=%d, users=%d, ratings=%d%n",
                books.count(), users.count(), ratings.count());
    }

    // ---------- path & parser helpers ----------

    private Path resolveOne(String... candidates) {
        Path baseDir = Paths.get(dataDir).toAbsolutePath().normalize();
        System.out.println("Import baseDir = " + baseDir);
        if (!Files.isDirectory(baseDir)) {
            throw new IllegalStateException("Data dir not found: " + baseDir);
        }
        for (String c : candidates) {
            Path p = baseDir.resolve(c);
            if (Files.exists(p)) return p;
        }
        throw new IllegalStateException("CSV not found in " + baseDir + " for " + java.util.Arrays.toString(candidates));
    }

    private char detectDelimiter(Path csv) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(csv, CSV_CHARSET)) {
            String header = br.readLine();
            if (header == null) return ',';
            long semis  = header.chars().filter(ch -> ch == ';').count();
            long commas = header.chars().filter(ch -> ch == ',').count();
            return semis > commas ? ';' : ',';
        }
    }

    private CsvParser newParser(char delim) {
        CsvParserSettings s = new CsvParserSettings();
        s.setHeaderExtractionEnabled(true);
        s.setLineSeparatorDetectionEnabled(true);
        s.setSkipEmptyLines(true);
        s.setNullValue(null);
        s.setEmptyValue("");
        s.setIgnoreLeadingWhitespaces(true);
        s.setIgnoreTrailingWhitespaces(true);
        s.setMaxCharsPerColumn(100_000);
        s.setMaxColumns(100);
        // tolerate odd quotes often present in BX files
        s.setParseUnescapedQuotes(true);

        CsvFormat f = s.getFormat();
        f.setDelimiter(delim);
        f.setQuote('"');
        f.setQuoteEscape('"'); // "" inside quoted fields

        return new CsvParser(s);
    }

    private Reader reader(Path p) throws IOException {
        return Files.newBufferedReader(p, CSV_CHARSET);
    }

    // ---------- importers (uniVocity-only) ----------

    private Map<String, Long> importBooks(Path csv) throws IOException {
        System.out.println("Importing books from: " + csv);
        Map<String, Long> map = new HashMap<>();
        CsvParser parser = newParser(detectDelimiter(csv));
        int i = 0;

        try (Reader r = reader(csv)) {
            for (Record row : parser.iterateRecords(r)) {
                String isbn13 = normalizeIsbn(val(row, "ISBN"));
                if (isbn13 == null) continue;

                Book b = books.findByIsbn13(isbn13).orElseGet(Book::new);
                b.setIsbn13(isbn13);
                b.setTitle(firstNonEmpty(row, "Book-Title", "Title"));
                b.setAuthor(firstNonEmpty(row, "Book-Author", "Author"));
                b.setCoverS(firstNonEmpty(row, "Image-URL-S", "ImageURLS"));
                b.setCoverM(firstNonEmpty(row, "Image-URL-M", "ImageURLM"));
                b.setCoverL(firstNonEmpty(row, "Image-URL-L", "ImageURLL"));
                books.save(b);
                map.put(isbn13, b.getId());

                if (++i % 5_000 == 0) {
                    em.flush(); em.clear();
                    System.out.println("Books imported: " + i);
                }
            }
        }
        em.flush(); em.clear();
        System.out.println("Books imported total: " + i);
        return map;
    }

    private Map<String, Long> importUsers(Path csv) throws IOException {
        System.out.println("Importing users from: " + csv);
        Map<String, Long> map = new HashMap<>();
        CsvParser parser = newParser(detectDelimiter(csv));
        int i = 0;

        try (Reader r = reader(csv)) {
            for (Record row : parser.iterateRecords(r)) {
                String extId = firstNonEmpty(row, "User-ID", "UserID");
                if (extId == null || extId.isBlank()) continue;

                String email = "u" + extId + "@example.local";
                AppUser u = users.findByEmail(email).orElseGet(AppUser::new);
                u.setEmail(email);
                u.setPass_hash("{noop}x"); // placeholder
                u.setRole("USER");
                users.save(u);
                map.put(extId, u.getId());

                if (++i % 5_000 == 0) {
                    em.flush(); em.clear();
                    System.out.println("Users imported: " + i);
                }
            }
        }
        em.flush(); em.clear();
        System.out.println("Users imported total: " + i);
        return map;
    }

    private void importRatings(Path csv, Map<String, Long> isbnToId, Map<String, Long> userMap) throws IOException {
        System.out.println("Importing ratings from: " + csv);
        CsvParser parser = newParser(detectDelimiter(csv));
        int inserted = 0;

        try (Reader r = reader(csv)) {
            for (Record row : parser.iterateRecords(r)) {
                String extUser = firstNonEmpty(row, "User-ID", "UserID");
                String isbn13 = normalizeIsbn(val(row, "ISBN"));
                if (extUser == null || isbn13 == null) continue;

                Long userId = userMap.get(extUser);
                Long bookId = isbnToId.get(isbn13);
                if (userId == null || bookId == null) continue;

                String ratingStr = firstNonEmpty(row, "Book-Rating", "Rating");
                if (ratingStr == null) continue;

                short rv;
                try { rv = Short.parseShort(ratingStr.trim()); } catch (Exception ignore) { continue; }

                Rating ent = new Rating();
                ent.setUserId(userId);
                ent.setBookId(bookId);
                ent.setRating(rv);
                ratings.save(ent);
                inserted++;

                if (inserted % 20_000 == 0) {
                    em.flush(); em.clear();
                    System.out.println("Ratings inserted: " + inserted);
                }
            }
        }
        em.flush(); em.clear();
        System.out.println("Ratings inserted total: " + inserted);
    }

    // ---------- helpers ----------

    private static String val(Record row, String col) {
        try {
            String v = row.getString(col);
            return (v == null || v.isBlank()) ? null : v.trim();
        } catch (Exception e) {
            return null; // column name not present
        }
    }

    private static String firstNonEmpty(Record row, String... cols) {
        for (String c : cols) {
            String v = val(row, c);
            if (v != null) return v;
        }
        return null;
    }

    // ISBN10 -> ISBN13 normalize (best-effort)
    private static String normalizeIsbn(String isbn) {
        if (isbn == null) return null;
        String digits = isbn.replaceAll("[^0-9Xx]", "");
        if (digits.length() == 13) return digits;
        if (digits.length() == 10) {
            String core = "978" + digits.substring(0, 9);
            int sum = 0; int[] w = {1, 3};
            for (int i = 0; i < 12; i++) sum += (core.charAt(i) - '0') * w[i % 2];
            int check = (10 - (sum % 10)) % 10;
            return core + check;
        }
        return null;
    }
}
