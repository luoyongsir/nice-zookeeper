# nice-zookeeper
Zookeeper客户端Curator的封装

使用方法如下：

pom.xml 依赖添加

    <dependency>
        <groupId>com.nice</groupId>
        <artifactId>nice-zookeeper</artifactId>
        <version>current.version</version>
    </dependency>
    
    <dependency>
        <groupId>org.springframework</groupId>
        <artifactId>spring-context</artifactId>
        <version>your.version</version>
    </dependency>

    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>your.version</version>
    </dependency>


添加配置

#zookeeper 可选配置<br/>
zookeeper.maxRetries=3<br/>
zookeeper.baseSleepTimeMs=1000<br/>
zookeeper.sessionTimeoutMs=60000<br/>
zookeeper.connectionTimeoutMs=15000<br/>

#zookeeper 必填配置<br/>
zookeeper.address=ip1:port1,ip2:port2,ip3:port3<br/>

