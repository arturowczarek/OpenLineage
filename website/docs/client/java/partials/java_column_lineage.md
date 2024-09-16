import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

### Hiding all to all lineage

:::info
This feature is available in OpenLineage versions >= 1.23.0.
:::

The `columnLineage.pruneFullIndirectColumnLineageEnabled` allows to hide the fields (columns) in scenarios where every
column
depends indirectly on every other column. The information about all-to-all (full) dependency can be always recreated
by the client from the column lineage status ``

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
columnLineage:
  pruneFullIndirectColumnLineageEnabled: true
```

</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                                                             | Definition                                                                                                                                                                                                            | Example |
|-----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| spark.openlineage.columnLineage.pruneFullIndirectColumnLineageEnabled | Determines if the list of fields should be included for fully connected columns with indirect relationship type. The fields can be recreated by the clients because the `fully_connected_indirect` status is present. | true    |

</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                                                       | Definition                                                                                                                                                                                                            | Example |
|-----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| openlineage.columnLineage.pruneFullIndirectColumnLineageEnabled | Determines if the list of fields should be included for fully connected columns with indirect relationship type. The fields can be recreated by the clients because the `fully_connected_indirect` status is present. | true    |

</TabItem>
</Tabs>
