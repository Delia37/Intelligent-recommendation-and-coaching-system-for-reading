package com.example.demo.identity;
import jakarta.persistence.*;
import lombok.Getter; import lombok.Setter;

@Entity @Table(name="users")
@Getter @Setter
public class AppUser {
    @Id @GeneratedValue(strategy=GenerationType.IDENTITY) Long id;
    @Column(unique=true, nullable=false) String email;
    @Column(nullable=false) String pass_hash;
    @Column(nullable=false) String role;

    public void setEmail(String email) {
        this.email = email;
    }

    public void setPass_hash(String s) {
        this.pass_hash = s;
    }

    public void setRole(String user) {
        this.role = user;
    }

    public Long getId() {
        return this.id;
    }
}
