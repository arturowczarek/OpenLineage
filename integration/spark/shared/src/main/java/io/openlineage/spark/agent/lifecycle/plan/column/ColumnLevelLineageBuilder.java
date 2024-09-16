/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFields;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetStatusBuilder;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.sql.ColumnMeta;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.jetbrains.annotations.NotNull;

/**
 * Builder class used to store information required to build {@link
 * ColumnLineageDatasetFacetFields}. Single instance of the class is passed when traversing logical
 * plan. It stores input fields, output fields and dependencies between the expressions in {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}. Dependency between expressions are used
 * to identify inputs used to evaluate specific output field.
 */
@Slf4j
public class ColumnLevelLineageBuilder {

  private Map<ExprId, Set<Dependency>> exprDependencies = new HashMap<>();
  private List<ExprId> datasetDependencies = new LinkedList<>();
  @Getter private Map<ExprId, Set<Input>> inputs = new HashMap<>();
  private Map<SchemaDatasetFacetFields, ExprId> outputs = new HashMap<>();
  private Map<ColumnMeta, ExprId> externalExpressionMappings = new HashMap<>();
  private final OpenLineage.SchemaDatasetFacet schema;
  private final OpenLineageContext context;

  private enum Statuses {
    FULLY_CONNECTED_INDIRECT_OPTIMIZATION_APPLIED(
        "fully_connected_indirect_optimization_applied",
        "The columns are fully connected with indirect type of dependency. All indirect dependencies were pruned.");

    private final String reason;
    private final String description;

    Statuses(String reason, String description) {
      this.reason = reason;
      this.description = description;
    }
  }

  /**
   * Indicates that every column has indirect connection to every other column. This is a shortcut
   * the clients can use if they want to detect this scenario. This field is extra information and
   * may not always be accurate, but it doesn't give false positives (if it is set to true, then
   * it's true; but when it is false you may not assume the fields are not fully connected). This
   * field was created for scenarios where the column lineage is huge and produce a lot of output
   * fields. It happens for example for wide tables where we have a DISTINCT clause. It is causing
   * an all-to-all relationship.
   */
  @Getter @Setter private boolean fullyConnectedIndirectOptimizationPossible = false;

  public ColumnLevelLineageBuilder(
      @NonNull final OpenLineage.SchemaDatasetFacet schema,
      @NonNull final OpenLineageContext context) {
    this.schema = schema;
    this.context = context;
  }

  /**
   * Adds input field.
   *
   * @param exprId
   * @param datasetIdentifier
   * @param attributeName
   */
  public void addInput(ExprId exprId, DatasetIdentifier datasetIdentifier, String attributeName) {
    inputs.computeIfAbsent(exprId, k -> new HashSet<>());
    inputs.get(exprId).add(new Input(datasetIdentifier, attributeName));
  }

  /**
   * Adds output field.
   *
   * @param exprId
   * @param attributeName
   */
  public void addOutput(ExprId exprId, String attributeName) {
    schema.getFields().stream()
        .filter(field -> field.getName().equals(attributeName))
        .findAny()
        .ifPresent(field -> outputs.putIfAbsent(field, exprId));
  }

  /**
   * Add dependency between outputExprId expression and inputExprId expression. Evaluation of
   * outputExprId requires inputExprId.
   *
   * @param outputExprId
   * @param inputExprId
   */
  public void addDependency(ExprId outputExprId, ExprId inputExprId) {
    exprDependencies
        .computeIfAbsent(outputExprId, k -> new HashSet<>())
        .add(new Dependency(inputExprId, TransformationInfo.identity()));
  }

  public void addDependency(
      ExprId outputExprId, ExprId inputExprId, TransformationInfo transformationInfo) {
    exprDependencies
        .computeIfAbsent(outputExprId, k -> new HashSet<>())
        .add(new Dependency(inputExprId, transformationInfo));
  }

  public void addDatasetDependency(ExprId outputExprId) {
    datasetDependencies.add(outputExprId);
  }

  public boolean hasOutputs() {
    return !outputs.isEmpty();
  }

  public Optional<ExprId> getOutputExprIdByFieldName(String field) {
    return outputs.keySet().stream()
        .filter(fields -> fields.getName().equals(field))
        .findAny()
        .map(f -> outputs.get(f));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    ObjectMapper mapper = OpenLineageClientUtils.newObjectMapper();
    try {
      sb.append("Inputs: ")
          .append(mapper.writeValueAsString(inputs))
          .append(System.lineSeparator());
      sb.append("Dependencies: ")
          .append(
              mapper.writeValueAsString(
                  exprDependencies.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              e -> e.toString())) // need to call toString method explicitly
                  ))
          .append(System.lineSeparator());

      sb.append("Outputs: ")
          .append(
              mapper.writeValueAsString(
                  outputs.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              e -> e.toString())) // need to call toString method explicitly
                  ))
          .append(System.lineSeparator());
    } catch (JsonProcessingException e) {
      sb.append("Unable to serialize: ").append(e.toString());
    }

    return sb.toString();
  }

  /**
   * Builds {@link ColumnLineageDatasetFacetFields} to be included in dataset facet.
   *
   * @return
   */
  public ColumnLineageDatasetFacetFields buildFields() {
    OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetFieldsBuilder();

    List<TransformedInput> datasetDependencyInputs =
        datasetDependencies.stream()
            .flatMap(e -> getInputsUsedFor(e).stream())
            .distinct()
            .collect(Collectors.toList());

    applyOptimizations(
            schema.getFields().stream()
                .map(field -> Pair.of(field, getInputsUsedFor(field.getName())))
                .filter(pair -> !pair.getRight().isEmpty())
                .map(
                    pair ->
                        Pair.of(
                            pair.getLeft(),
                            facetInputFields(pair.getRight(), datasetDependencyInputs))))
        .forEach(
            pair ->
                fieldsBuilder.put(
                    pair.getLeft().getName(),
                    context
                        .getOpenLineage()
                        .newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(pair.getRight())
                        .build()));

    return fieldsBuilder.build();
  }

  /**
   * Applies extra optimizations on the column lineage fields. The optimizations may not always be
   * applicable. The only possible optimization at the moment is full indirect column lineage
   * pruning.
   *
   * @param fields fields to be optimized
   * @return optimized fields
   */
  private Stream<
          Pair<
              SchemaDatasetFacetFields, List<ColumnLineageDatasetFacetFieldsAdditionalInputFields>>>
      applyOptimizations(
          Stream<
                  Pair<
                      SchemaDatasetFacetFields,
                      List<ColumnLineageDatasetFacetFieldsAdditionalInputFields>>>
              fields) {
    /*
    If we have fully connected columns (every column depends on every column), we may want to skip producing
    the fields list, because the clients can recreate this information from the schema and the status field.
    This behavior is controlled by the "spark.openlineage.columnLineage.skipFullyConnectedColumnsLineage.enabled" property.
     */
    return applyFullIndirectLineagePruning()
        ? FullIndirectColumnPruning.apply(fields, context.getOpenLineage())
        : fields;
  }

  public boolean applyFullIndirectLineagePruning() {
    boolean fullIndirectColumnsLineage = isFullyConnectedIndirectOptimizationPossible();
    boolean hideFullIndirectLineageFields =
        context
            .getOpenLineageConfig()
            .getColumnLineageConfig()
            .isPruneFullIndirectColumnLineageEnabled();
    boolean applyFullIndirectLineagePruning =
        fullIndirectColumnsLineage && hideFullIndirectLineageFields;
    log.debug(
        "fullIndirectColumnsLineage: [{}], hideFullIndirectLineageFields config: [{}]. Applying full indirect lineage pruning: [{}]",
        fullIndirectColumnsLineage,
        hideFullIndirectLineageFields,
        applyFullIndirectLineagePruning);
    return applyFullIndirectLineagePruning;
  }

  public OpenLineage.ColumnLineageDatasetFacetStatus buildStatus() {
    ColumnLineageDatasetFacetStatusBuilder statusBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetStatusBuilder();
    if (applyFullIndirectLineagePruning()) {
      statusBuilder.put(
          Statuses.FULLY_CONNECTED_INDIRECT_OPTIMIZATION_APPLIED.name(),
          context
              .getOpenLineage()
              .newColumnLineageDatasetFacetStatusAdditionalBuilder()
              .reason(Statuses.FULLY_CONNECTED_INDIRECT_OPTIMIZATION_APPLIED.reason)
              .description(Statuses.FULLY_CONNECTED_INDIRECT_OPTIMIZATION_APPLIED.description)
              .build());
    }
    return statusBuilder.build();
  }

  private List<ColumnLineageDatasetFacetFieldsAdditionalInputFields> facetInputFields(
      List<TransformedInput> inputFields, List<TransformedInput> datasetDependencyInputs) {
    Map<Input, List<TransformedInput>> combinedInputs = new HashMap<>();
    inputFields.stream()
        .forEach(e -> combinedInputs.computeIfAbsent(e.getInput(), k -> new LinkedList<>()).add(e));
    datasetDependencyInputs.stream()
        .forEach(e -> combinedInputs.computeIfAbsent(e.getInput(), k -> new LinkedList<>()).add(e));

    return combinedInputs.entrySet().stream()
        .map(
            field ->
                new OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFieldsBuilder()
                    .namespace(field.getKey().getDatasetIdentifier().getNamespace())
                    .name(field.getKey().getDatasetIdentifier().getName())
                    .field(field.getKey().getFieldName())
                    .transformations(
                        field.getValue().stream()
                            .map(TransformedInput::getTransformationInfo)
                            .map(TransformationInfo::toInputFieldsTransformations)
                            .collect(Collectors.toList()))
                    .build())
        .collect(Collectors.toList());
  }

  List<TransformedInput> getInputsUsedFor(String outputName) {
    Optional<SchemaDatasetFacetFields> outputField =
        schema.getFields().stream()
            .filter(field -> field.getName().equalsIgnoreCase(outputName))
            .findAny();
    if (!outputField.isPresent() || !outputs.containsKey(outputField.get())) {
      return Collections.emptyList();
    }

    ExprId outputExprId = outputs.get(outputField.get());
    return getInputsUsedFor(outputExprId);
  }

  @NotNull
  private List<TransformedInput> getInputsUsedFor(ExprId outputExprId) {
    List<TransformedInput> collect =
        findDependentInputs(outputExprId).stream()
            .filter(dependency -> inputs.containsKey(dependency.getExprId()))
            .flatMap(
                dependency ->
                    inputs.get(dependency.getExprId()).stream()
                        .map(e -> new TransformedInput(e, dependency.getTransformationInfo())))
            .distinct()
            .collect(Collectors.toList());
    return collect;
  }

  private List<Dependency> findDependentInputs(ExprId outputExprId) {
    List<Dependency> dependentInputs = new LinkedList<>();
    dependentInputs.add(new Dependency(outputExprId, TransformationInfo.identity()));
    boolean continueSearch = true;

    Set<Dependency> newDependentInputs =
        Collections.singleton(new Dependency(outputExprId, TransformationInfo.identity()));
    while (continueSearch) {
      newDependentInputs =
          newDependentInputs.stream()
              .filter(dependency -> exprDependencies.containsKey(dependency.getExprId()))
              .flatMap(
                  dependency ->
                      exprDependencies.get(dependency.getExprId()).stream().map(dependency::merge))
              .filter(dependency -> !dependentInputs.contains(dependency)) // filter already added
              .collect(Collectors.toSet());

      dependentInputs.addAll(newDependentInputs);
      continueSearch = !newDependentInputs.isEmpty();
    }

    return dependentInputs;
  }

  public void addExternalMapping(ColumnMeta meta, ExprId exprid) {
    externalExpressionMappings.putIfAbsent(meta, exprid);
  }

  public ExprId getMapping(ColumnMeta columnMeta) {
    return externalExpressionMappings.get(columnMeta);
  }
}
