package com.example.demo.debug;

import com.example.demo.catalog.BookRepository;
import com.example.demo.identity.AppUserRepository;
import com.example.demo.catalog.RatingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class DebugController {
    private final BookRepository books;
    private final AppUserRepository users;
    private final RatingRepository ratings;

    @GetMapping("/debug/counts")
    public Map<String, Object> counts() {
        return Map.of(
                "books", books.count(),
                "users", users.count(),
                "ratings", ratings.count()
        );
    }
}
