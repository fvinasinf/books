package com.books;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

import static com.mongodb.client.model.Filters.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.stream.Collectors;


import org.bson.Document;
import org.bson.conversions.Bson;
/**
 * To execute this class is mandatory to execute this cli command:
 * podman run --detach --replace --name data_db -p 3000:27017 -v database_vol:/data/db/ docker.io/mongodb/mongodb-community-server:latest 
 * And to populate the Mongo Database with fresh new data with the provided books.json file.
 * I used MongoDB Compass as DB manager, but could be done with any DB manager of your choice.
*/
public class App {
    private static final String DELIMITER = ",";
    private static final int TOP_THREE = 3;
    private static final int ALL_ITERS = -1;
    private static final String TITLES_AUTHORS_CSV = "titlesAuthors.csv";
    private static final String TO_TIME_MILIS = "000";
    private static final String CONN_CHAIN = "mongodb://localhost:3000/";
    private static final String DATABASE = "Books";
    private static final String COLLECTION = "Books";

    public static void main( String[] args ) {
        try (MongoClient client = MongoClients.create(CONN_CHAIN)) {
            MongoDatabase booksDB = client.getDatabase(DATABASE);
            MongoCollection<Document> books = booksDB.getCollection(COLLECTION);

            /* Mandatory */
            getFourHundredBooksAndHarryInTitleBooks(books);
            getJKRowlingBooks(books);
            alphaOrderedAndCountByAuthor(books);
            changeDateFormat(books);
            avgMinMaX(books);
            addWordCountShowBooksByAuthor(books);

            /* Optionals*/
            duplicateStudentsAndBooksWithoutTimestamp(books);
            mostRecentBooks(books);
            titlesAuthorsWithCsv(books);
        }
    }

    /* Filtra los libros con más de 400 páginas y aquellos cuyo título contenga "Harry" */
    private static void getFourHundredBooksAndHarryInTitleBooks(MongoCollection<Document> books) {
       showResults(
            "fourHundredBooksAndHarryInTitleBooks", 
            books
                .find(
                    Filters.and(
                        gt("pages", 400), 
                        regex("title", ".*Harry*.")
                    )
                ).iterator()
        );
    }

    /*Obtén los libros escritos por "J.K. Rowling".*/
    private static void getJKRowlingBooks(MongoCollection<Document> books) {
        showResults(
            "jkRowlingBooks", 
            books
                .find(eq("author.name", "J.K. Rowling"))
                .iterator()
        );
    }

    /* Lista los títulos ordenados alfabéticamente y cuenta cuántos libros ha escrito cada autor.*/
    private static void alphaOrderedAndCountByAuthor(MongoCollection<Document> books)  {
        showResults(
            "Alpha ordered",
            books
                .find()
                .sort(Sorts.ascending("title"))
                .iterator()
        );


        showResults(
            "Books by author", 
            books
                .aggregate(
                    List.of(
                        Aggregates.group(
                            "$author.name", 
                            Accumulators.sum(
                                "titleCount", 
                                1
                            )
                        )
                    )
                ).iterator()
        );
    }

    /*Convierte publicationTimestamp a formato AAAAMMDD .*/
    private static void changeDateFormat(MongoCollection<Document> books) {
        try {
            Map<String, String> dateTransformation = new HashMap<>(); 
            MongoCursor<Document> documents =
                books
                    .find(exists("publicationTimestamp", true))
                    .iterator();

            while (documents.hasNext()) {
                Document document = documents.next();
                
                String dateTransformed = (String) document.get("publicationTimestamp") + TO_TIME_MILIS;
                String output = formatDate(dateTransformed);

                dateTransformation.put(dateTransformed, output);
            }
            
            for (Entry<String, String> conversion: dateTransformation.entrySet()) {
                books
                    .updateOne(
                        eq(
                            "publicationTimestamp", 
                            conversion.getKey().substring(0, conversion.getKey().length() - TO_TIME_MILIS.length())
                        ), 
                        Updates.set("publicationTimestamp", conversion.getValue())
                    );
            }
        } catch (ParseException e) {
            System.out.println("Error at parsing!");
        } catch (NumberFormatException e) {
            System.out.println("Error at formatting numbers!");
        }
        showResults(
            "Dates transformed", 
            books
                .find()
                .iterator()
        );
    }

    private static String formatDate (String dateToFormat) throws ParseException, NumberFormatException {
        SimpleDateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
        inputFormat.setTimeZone(TimeZone.getTimeZone("CET"));
        
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-dd-MM");
        
        Date formatted = inputFormat.parse(new Date(Long.parseLong(dateToFormat)).toString());
        
        return outputFormat.format(formatted);
    }

    /*Calcula el promedio de páginas y encuentra el libro con más y menos páginas.*/
    private static void avgMinMaX(MongoCollection<Document> books)  {
        showResults(
            "Average pages", 
            books
                .aggregate(
                    List.of(
                        Aggregates.group(
                            "", 
                            Accumulators.avg(
                                "avg_pages", 
                                "$pages"
                            )
                        )
                    )
                ).iterator()
        );

        Document doc = 
            books
                .aggregate(
                    List.of(
                        Aggregates.group(
                            "_", 
                            Accumulators.max(
                                "max_pages", 
                                "$pages"
                            )
                        )
                    )
                ).iterator()
                .next();
        Integer maxPages = (Integer) doc.get("max_pages");

        showResults(
            "Max pages", 
            books
                .find(eq("pages", maxPages))
                .iterator()
        );

        doc = 
            books.aggregate(
                List.of(
                    Aggregates.group(
                        "_", 
                        Accumulators.min(
                            "min_pages", 
                            "$pages"
                        )
                    )
                )
            ).iterator()
            .next();
        Integer minPages = (Integer) doc.get("min_pages");

        showResults(
            "Min pages", 
            books
                .find(eq("pages", minPages))
                .iterator()
        );
    }

    /* Añade un campo wordCount 250 palabras por página) y agrupa los libros por autor. */
    private static void addWordCountShowBooksByAuthor(MongoCollection<Document> books) {
        MongoCursor<Document> documents = 
            books
                .aggregate(
                    List.of(
                        Aggregates.group(
                            "$author.name", 
                            Accumulators.sum(
                                "word_count", 
                                new Document(
                                    "$multiply", 
                                    List.of("$pages", 250)
                                )
                            )
                        )
                    )
                )
                .iterator();
        
        while (documents.hasNext()) {
            Document document = documents.next();
            books
                .updateMany(
                    eq("author.name", document.get("_id")), 
                    Updates.set("wordCount", document.get("word_count"))
                );
        }
                
    }

    /*(Opcional) Verifica si hay autores duplicados y encuentra los libros sin publicationTimestamp.*/
    private static void duplicateStudentsAndBooksWithoutTimestamp(MongoCollection<Document> books) {

        MongoCursor<Document> documents = 
            books.aggregate(
                List.of(
                    Aggregates.group(
                        "$author.name", 
                        Accumulators.sum("titleCount", 1)
                    )
                )
            ).iterator();

        while (documents.hasNext()) {
            Document document = documents.next();
            if ((Integer) document.get("titleCount") > 1) {
                System.out.println(String.format("More than one occurrence: %s", document.get("_id")));
            }
        }

        showResults(
            "No timestamps", 
            books
                .find(exists("publicationTimestamp", false))
                .iterator()
        );
    }

    /* (Opcional) Identifica los libros más recientes. */
    private static void mostRecentBooks(MongoCollection<Document> books) {
        showResults(
            "Date ordered recent",
            books
                .find(exists("publicationTimestamp", true))
                .sort(Sorts.descending("publicationTimestamp"))
                .iterator(),
            TOP_THREE
        );
    }

    /* (Opcional) Genera un JSON con títulos y autores y exporta la lista a CSV ( id , title , author_name , pages ) */
    private static void titlesAuthorsWithCsv(MongoCollection<Document> books) {
        Bson projection = Projections.include("id", "title", "author.name", "pages");
        MongoCursor<Document> documents = 
            books
                .find()
                .projection(projection)
                .iterator();
        generateCSV(documents);
    }

    private static void generateCSV(MongoCursor<Document> documents) {
        /* Transform to CSV */
        try {
            Files.deleteIfExists(Path.of(TITLES_AUTHORS_CSV));
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeCSVHeader();
        /* Writin body */
        while (documents.hasNext()) {
            Document document = documents.next();
            documentAsCSV(document);
        }
        System.out.println(String.format("CSV generated at %s", TITLES_AUTHORS_CSV));
    }

    private static void writeCSVHeader() {
        String line =
            List.of(
                "id", 
                "title", 
                "author_name", 
                "pages"
            ).stream().collect(Collectors.joining(DELIMITER));
        writeOnFile(line);
    }

    private static void documentAsCSV(Document document) {
        String line =
            List.of(
                ((Integer) document.get("id")).toString(), 
                (String) document.get("title"), 
                (String) ((Document) document.get("author")).get("name"), 
                ((Integer) document.get("pages")).toString()
            ).stream().collect(Collectors.joining(DELIMITER));
        writeOnFile(line);
    }

    private static void writeOnFile(String line) {
        try {
            Files.writeString(
                Path.of(TITLES_AUTHORS_CSV), 
                line, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.APPEND
            );
            Files.writeString(
                Path.of(TITLES_AUTHORS_CSV), 
                System.lineSeparator(), 
                StandardOpenOption.CREATE, 
                StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void showResults(String resultName, MongoCursor<Document> cursor) {
        showResults(resultName, cursor, ALL_ITERS);
    }

    private static void showResults(String resultName, MongoCursor<Document> cursor, int howMany) {
        List<String> results = new ArrayList<>();
        int read = 0;
        while (cursor.hasNext()) {
            results.add(cursor.next().toString());
            if (howMany != ALL_ITERS && read >= howMany - 1) {
                break;
            }
            read++;
        }
        System.out.println(
            String.format(
                "%s : \n%s", 
                resultName, 
                results.stream().collect(Collectors.joining("\n - Next item: \n"))
            )
        );
    }
}
