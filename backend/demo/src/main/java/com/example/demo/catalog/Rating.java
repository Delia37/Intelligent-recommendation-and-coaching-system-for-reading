package com.example.demo.catalog;
import jakarta.persistence.*;
import lombok.Getter; import lombok.Setter;
import java.time.Instant;

@Entity @Table(name="ratings")
@Getter @Setter
@IdClass(RatingKey.class)
public class Rating {
    @Id Long userId;
    @Id Long bookId;
    short rating;
    Instant ratedAt = Instant.now();

//    public void setUserId(Long userId) {
//        this.userId = userId;
//    }
//
//    public void setBookId(Long bookId) {
//        this.bookId = bookId;
//    }
//
//    public void setRating(short rv) {
//        this.rating = rv;
//    }
}
