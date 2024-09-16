package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@UtilityClass
public class FullIndirectColumnPruning {

  public static Stream<
          Pair<
              OpenLineage.SchemaDatasetFacetFields,
              List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields>>>
      apply(
          Stream<
                  Pair<
                      OpenLineage.SchemaDatasetFacetFields,
                      List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields>>>
              step,
          OpenLineage openLineage) {
    return step.map(
        pair -> {
          List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields> withoutIndirect =
              pair.getRight().stream()
                  .map(
                      inputField ->
                          cloneWithTransformations(
                              inputField, filterOutIndirectDependencies(inputField), openLineage))
                  .filter(inputField -> !inputField.getTransformations().isEmpty())
                  .collect(Collectors.toList());
          return Pair.of(pair.getLeft(), withoutIndirect);
        });
  }

  private static @NotNull List<
          OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFieldsTransformations>
      filterOutIndirectDependencies(
          OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields inputField) {
    return inputField.getTransformations().stream()
        .filter(t -> !TransformationInfo.Types.INDIRECT.toString().equals(t.getType()))
        .collect(Collectors.toList());
  }

  /**
   * Clones input field object with new transformations list
   *
   * @param inputField the source input field object
   * @param noIndirectTransformations the list of transformations for a new object
   * @return the cloned input field object with new transformations list
   */
  private static OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields
      cloneWithTransformations(
          OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields inputField,
          List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFieldsTransformations>
              noIndirectTransformations,
          OpenLineage openLineage) {
    OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFieldsBuilder builder =
        openLineage
            .newColumnLineageDatasetFacetFieldsAdditionalInputFieldsBuilder()
            .namespace(inputField.getNamespace())
            .name(inputField.getName())
            .field(inputField.getField())
            .transformations(noIndirectTransformations);
    inputField.getAdditionalProperties().forEach(builder::put);
    return builder.build();
  }
}
