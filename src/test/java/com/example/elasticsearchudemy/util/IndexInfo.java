package com.example.elasticsearchudemy.util;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class IndexInfo {
    @JsonProperty("_id")
    private String id;
    @JsonProperty("_type")
    private String type;
    @JsonProperty("_index")
    private String index;


}
