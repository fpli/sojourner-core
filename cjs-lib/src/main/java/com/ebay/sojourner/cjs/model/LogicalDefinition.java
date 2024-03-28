package com.ebay.sojourner.cjs.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;

@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
public class LogicalDefinition {

    @NotBlank
    private String platform;

    @NotBlank
    private UUIDGenerator uuidGenerator;

    @NotEmpty
    private List<EventClassifier> eventClassifiers;

    private List<FieldExtractor> fields;
}
