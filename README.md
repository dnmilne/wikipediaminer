wikipediaminer
==============

An open source toolkit for mining Wikipedia forked from: https://github.com/dnmilne/wikipediaminer

Contain some improvements in the WebServices and a lot of bugfixes to Milne's sources.

 Documentation at : https://github.com/dnmilne/wikipediaminer/wiki


TODO:
```list
Add support for live snapshots of wikipedia (DBPedia approach) to stay updated
Implement other disambigation approaches like http://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=6354382
Support for binary data on the webServices, (Thrift for example) to avoid problems with UTF8 characters.
```


Add this repository to your POM.xml.

```xml

 <repository>
        <id>galan-maven-repo</id>
        <name>galan-maven-repo-releases</name>
        <url>http://galan.ehu.es/artifactory/ext-release-local</url>
    </repository>

```

Then...

```xml

 <repository>
        <id>galan-maven-repo</id>
        <name>galan-maven-repo-releases</name>
        <url>http://galan.ehu.es/artifactory/ext-release-local</url>
 </repository>

```

Then add the required subproyect, for example...
```xml
<dependency>
    <groupId>org.wikipedia-miner</groupId>
            <artifactId>wikipedia-miner-core</artifactId>
            <version>1.2.4</version>
</dependency>




