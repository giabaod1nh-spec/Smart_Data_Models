package com.traffic.server.config;

import com.traffic.server.entity.UserAccount;
import com.traffic.server.repository.UserAccountRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.password.PasswordEncoder;


@Configuration
public class AdminUserSeeder {

    @Value("${app.security.admin.username}")
    private String adminUsername;

    @Value("${app.security.admin.password}")
    private String adminPassword;

    @Bean
    public CommandLineRunner seedAdminUser(UserAccountRepository repository, PasswordEncoder passwordEncoder) {
        return args -> {
            if (!repository.existsByUsername(adminUsername)) {
                repository.save(UserAccount.builder()
                        .username(adminUsername)
                        .password(passwordEncoder.encode(adminPassword))
                        .build());
            }
        };
    }
}
