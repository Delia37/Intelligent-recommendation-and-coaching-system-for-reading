package com.example.demo.catalog;
import org.springframework.data.jpa.repository.JpaRepository;
public interface RatingRepository extends JpaRepository<Rating, RatingKey> {}