#!/bin/bash

# Loading the Data
java -cp target/classes gr.aueb.data_mingler_optimizations.util.DvmToNeo4jLoader src/main/resources/xml_files/default.dvm.xml CLEAR

# Executing the query
java -cp target/classes gr.aueb.data_mingler_optimizations.QueryEvaluation src/main/resources/xml_files/driver_query.xml NONE INTERSECT
