package com.example.demo.catalog;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "books")
@Getter @Setter
public class Book {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "isbn13", unique = true)
    private String isbn13;

    @Column(nullable = false)
    private String title;

    @Column(nullable = false)
    private String author;

    @Column(columnDefinition = "text")
    private String description;

    @Column(name = "cover_s")
    private String coverS;

    @Column(name = "cover_m")
    private String coverM;

    @Column(name = "cover_l")
    private String coverL;

    @Column(name = "page_count")
    private Integer pageCount;
}
