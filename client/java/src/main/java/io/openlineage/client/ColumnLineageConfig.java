package io.openlineage.client;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class ColumnLineageConfig {
  boolean pruneFullIndirectColumnLineageEnabled;
}
