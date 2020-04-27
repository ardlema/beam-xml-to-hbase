## Beam examples

This project includes some Apache Beam examples

### XML to HBase

Reading a simple XML file (located within the root folder of the project), extract the information from the XML elements and store it in HBase.

Prerequisites: A local instance of HBase is up & running

How to compile and execute using the direct runner

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.XMLInfoExtractor -Dexec.args="--inputFile=employees.xml --output=counts" -Pdirect-runner
```