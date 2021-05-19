package com.example.elasticsearchudemy.util;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Setter
@Getter
@NoArgsConstructor
public class MovieInfo {
    private String id;
    private String title;
    private Long year;
    private String[] genre;
}
