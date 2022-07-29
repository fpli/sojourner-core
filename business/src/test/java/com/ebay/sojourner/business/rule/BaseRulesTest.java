package com.ebay.sojourner.business.rule;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;

public class BaseRulesTest<T> {

  private RulesTestInputObjects rulesInput;
  private RulesTestCase rulesTestCase;
  private List<RulesTestCase> rulesTestCaseList;

  private ObjectMapper objectMapper =
      new ObjectMapper(new YAMLFactory())
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  protected List<RulesTestCase> loadTestCases(String fileName) throws Exception {
    rulesTestCaseList = Lists.newArrayList();
    String fullPathName = "src/test/resources/test-cases/rules/" + fileName;
    File resourcesFile = new File(fullPathName);
    JsonNode jsonNode = objectMapper.readTree(resourcesFile);
    for (int i = 0; i < jsonNode.size(); i++) {
      int caseNum = i;
      rulesTestCase = new RulesTestCase();
      rulesInput = new RulesTestInputObjects();
      String typeName = jsonNode.get(caseNum).get("type").asText();
      Class<?> clazz = Class.forName("com.ebay.sojourner.common.model." + typeName);
      Object inputObjectValue =
          objectMapper.treeToValue(jsonNode.get(caseNum).get("input").get(typeName), clazz);
      Field inputObjectType =
          rulesInput
              .getClass()
              .getDeclaredField(typeName.substring(0, 1).toLowerCase() + typeName.substring(1));
      inputObjectType.setAccessible(true);
      inputObjectType.set(rulesInput, inputObjectValue);

      rulesTestCase.setInput(rulesInput);
      rulesTestCase.setName(jsonNode.get(caseNum).get("name").asText());
      rulesTestCase.setType(typeName);
      rulesTestCase.setExpectResult(jsonNode.get(caseNum).get("expectResult").asInt());
      rulesTestCaseList.add(rulesTestCase);
    }
    return rulesTestCaseList;
  }

  protected List<DynamicTest> generateDynamicTests(List<RulesTestCase> testCases, Rule<T> rule) {
    List<DynamicTest> dynamicTestList = Lists.newArrayList();
    for (int i = 0; i < testCases.size(); i++) {
      int caseNum = i;
      DynamicTest dynamicTest =
          DynamicTest.dynamicTest(
              testCases.get(i).getName(),
              () -> {
                RulesTestCase rulesTestCase = testCases.get(caseNum);
                String typeName = rulesTestCase.getType();
                RulesTestInputObjects rulesInput = rulesTestCase.getInput();
                Field inputObjectType =
                    rulesInput
                        .getClass()
                        .getDeclaredField(
                            typeName.substring(0, 1).toLowerCase() + typeName.substring(1));
                inputObjectType.setAccessible(true);
                T t = (T) inputObjectType.get(rulesInput);
                rule.init();
                int actual = rule.getBotFlag(t);
                Assertions.assertThat(actual).isEqualTo(rulesTestCase.getExpectResult());
              });

      dynamicTestList.add(dynamicTest);
    }
    return dynamicTestList;
  }
}
