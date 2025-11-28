package com.example.demo.catalog;
import java.io.Serializable;
import java.util.Objects;

import lombok.*;
@Getter @Setter
public class RatingKey implements Serializable {
    Long userId; Long bookId;
    public RatingKey() {}
    public RatingKey(Long userId, Long bookId) { this.userId = userId; this.bookId = bookId; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RatingKey rk)) return false;
        return Objects.equals(userId, rk.userId) && Objects.equals(bookId, rk.bookId);
    }
    @Override public int hashCode() { return Objects.hash(userId, bookId); }
}
