# gswagon-maven-plugin
The `gswagon-maven-plugin` provides wagon support for Google Cloud Storage.

It is intended for use in scenarios where the accessor has equal rights to all items in the repository. For example, read-only everything or read-write everything.

## Configuration
|Property|Description|Required|
|--------|-----------|--------|
|`wagon.gs.project.${repository_id}`|The project ID to use for the GCS repository|Yes|
For example, if your reposoitory ID is `gs_dev` then your propety would be `wagon.gs.project.gs_dev`
## Repository
The respository format is expected to be in the following format:

```
gs://bucket/prefix
```
Where `bucket` is the bucket containing the jars, and `prefix` is the