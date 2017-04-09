# gswagon-maven-plugin
The `gswagon-maven-plugin` provides wagon support for Google Cloud Storage.

It is intended for use in scenarios where the accessor has equal rights to all items in the repository. For example, read-only everything or read-write everything.

## Usage
To use the maven deploy plugin be sure to include the repository with a `url` starting with a `gs` scheme as per.
```xml
<project>
  <distributionManagement>
    <repository>
      <id>${repository_id}</id>
      <url>gs://test-bucket-7481395/test</url>
    </repository>
  </distributionManagement>
</project>
```

The build should have the following extension

```xml
<project>
  <build>
    <extensions>
      <extension>
        <groupId>com.allennet.maven.plugins</groupId>
        <artifactId>gswagon-maven-plugin</artifactId>
        <version>${gswagon-maven-plugin.version}</version>
      </extension>
    </extensions>
  </build>
</project>
```

In addition the following property must be set because I can't figure out how to pass extension properties into a plugin :-/

|Property|Description|Required|
|--------|-----------|--------|
|`wagon.gs.project.${repository_id}`|The project ID to use for the GCS repository|Yes|

For example, if your reposoitory ID is `gs_dev` then your propety would be `wagon.gs.project.gs_dev`

Lastly, make sure the machine can assume the correct [Google IAM](https://developers.google.com/identity/protocols/application-default-credentials). The easiest way is to make sure `GOOGLE_APPLICATION_CREDENTIALS` references the correct service account json which already has the desired granted privilages on the bucket of interest.

## Repository
The respository format is expected to be in the following format:

```
gs://bucket/prefix
```
Where `bucket` is the bucket containing the jars, and `prefix` is the
