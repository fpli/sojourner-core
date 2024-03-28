package com.ebay.sojourner.cjs.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SignalDefinition implements Serializable {


    @NotNull
    private String type;

    @NotNull
    private String domain;

    @NotBlank
    private String name;

    @NotEmpty
    private List<LogicalDefinition> logicalDefinition;

}
