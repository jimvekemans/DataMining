import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

// 0: index
// 1: User-ID
// 2: User-Location
// 3: User-Age
// 4: ISBN
// 5: Book-Rating
// 6: Book-Title
// 7: Book-Author
// 8: Book-Year-Of-Publication
// 9: Book-Publisher
public class Book {
    private String ISBN;
    private int bookRating;
    private String bookTitle;
    private String bookAuthor;
    private int bookPublicationYear;
    private String bookPublisher;

    public Book(String CSVRow){
        List<String> CSVRowItems = Arrays.stream(CSVRow.split("\t")).collect(Collectors.toList());
        setISBN(CSVRowItems.get(4));
        setBookRating(Integer.parseInt(CSVRowItems.get(5)));
        setBookTitle(CSVRowItems.get(6));
        setBookAuthor(CSVRowItems.get(7));
        setBookPublicationYear(Integer.parseInt(CSVRowItems.get(8)));
        setBookPublisher(CSVRowItems.get(9));
    }

    public String getISBN() {
        return ISBN;
    }

    public void setISBN(String ISBN) {
        this.ISBN = ISBN;
    }

    public int getBookRating() {
        return bookRating;
    }

    public void setBookRating(int bookRating) {
        this.bookRating = bookRating;
    }

    public String getBookTitle() {
        return bookTitle;
    }

    public void setBookTitle(String bookTitle) {
        this.bookTitle = bookTitle;
    }

    public String getBookAuthor() {
        return bookAuthor;
    }

    public void setBookAuthor(String bookAuthor) {
        this.bookAuthor = bookAuthor;
    }

    public int getBookPublicationYear() {
        return bookPublicationYear;
    }

    public void setBookPublicationYear(int bookPublicationYear) {
        this.bookPublicationYear = bookPublicationYear;
    }

    public String getBookPublisher() {
        return bookPublisher;
    }

    public void setBookPublisher(String bookPublisher) {
        this.bookPublisher = bookPublisher;
    }
}
