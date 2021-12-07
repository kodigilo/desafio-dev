package com.example.cnab.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.*;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spi.service.contexts.SecurityContext;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Configuration
@EnableSwagger2
public class Config {



    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(metaData())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.example.cnab.controller"))
                .apis(RequestHandlerSelectors.any())
                .paths(PathSelectors.any())
                .build();
    }

    private ApiInfo metaData() {
        List<VendorExtension> vendorExtensions = new ArrayList<>();
        ApiInfo apiInfo = new ApiInfo(
                "API CNAB",
                "API CNAB",
                "1.0",
                "Terms of service",
                new Contact("CNAB", "https://www.linkedin.com/in/gabrielalencar/", "gabriel@alencar.site"),
                "Publica",
                "#",vendorExtensions );
        return apiInfo;
    }


}