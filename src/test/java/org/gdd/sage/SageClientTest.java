package org.gdd.sage;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.gdd.sage.cli.DescribeQueryExecutor;
import org.gdd.sage.cli.QueryExecutor;
import org.gdd.sage.engine.SageExecutionContext;
import org.gdd.sage.federated.factory.FederatedQueryFactory;
import org.gdd.sage.federated.factory.ServiceFederatedQueryFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class SageClientTest {

    @Ignore
    @Test
    public void bsbmQuery() {
        String url = "http://sage.univ-nantes.fr/sparql/bsbm1M";
        String queryString = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" +
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "\n" +
                "SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3\n" +
                " ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4 \n" +
                "WHERE {\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> rdfs:label ?label .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> rdfs:comment ?comment .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:producer ?p .\n" +
                "    ?p rdfs:label ?producer .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> dc:publisher ?p . \n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productFeature ?f .\n" +
                "    ?f rdfs:label ?productFeature .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productPropertyTextual1 ?propertyTextual1 .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productPropertyTextual2 ?propertyTextual2 .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productPropertyTextual3 ?propertyTextual3 .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productPropertyNumeric1 ?propertyNumeric1 .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productPropertyNumeric2 ?propertyNumeric2 .\n" +
                "    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productPropertyTextual1 ?propertyTextual4 }\n" +
                "    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productPropertyTextual5 ?propertyTextual5 }\n" +
                "    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product216> bsbm:productPropertyNumeric4 ?propertyNumeric4 }\n" +
                "}\n";
        Query query = QueryFactory.create(queryString);
        FederatedQueryFactory factory = new ServiceFederatedQueryFactory(url, query);
        factory.buildFederation();
        query = factory.getLocalizedQuery();
        Dataset dataset = factory.getFederationDataset();
        SageExecutionContext.configureDefault(ARQ.getContext());
        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            ResultSet results = qexec.execSelect();
            List<QuerySolution> solutions = new ArrayList<>();
            results.forEachRemaining(querySolution -> {
                System.out.println(querySolution);
                assertTrue("?propertyTextual4 should be bounded", querySolution.contains("propertyTextual4"));
                assertFalse("?propertyTextual5 should not be bounded", querySolution.contains("propertyTextual5"));
                assertFalse("?propertyNumeric4 should not be bounded", querySolution.contains("propertyNumeric4"));
                solutions.add(querySolution);
            });
            assertEquals("It should find 22 solutions bindings", 22, solutions.size());
        }
    }

    @Ignore
    @Test
    public void noSuchElementQuery() {
        String url = "http://172.16.8.50:8000/sparql/bsbm1k";
        String queryString = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" +
                "\n" +
                "SELECT DISTINCT ?product ?productLabel\n" +
                "WHERE {\n" +
                "\t?product rdfs:label ?productLabel .\n" +
                "\t<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productFeature ?prodFeature .\n" +
                "\t?product bsbm:productFeature ?prodFeature .\n" +
                "\t<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productPropertyNumeric1 ?origProperty1 .\n" +
                "\t?product bsbm:productPropertyNumeric1 ?simProperty1 .\n" +
                "\t<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productPropertyNumeric2 ?origProperty2 .\n" +
                "\t?product bsbm:productPropertyNumeric2 ?simProperty2 .\n" +
                "\tFILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> != ?product)\n" +
                "\tFILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))\n" +
                "\tFILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))\n" +
                "}\n" +
                "ORDER BY DESC(?productLabel)\n" +
                "LIMIT 5\n";
        Query query = QueryFactory.create(queryString);
        FederatedQueryFactory factory = new ServiceFederatedQueryFactory(url, query);
        factory.buildFederation();
        query = factory.getLocalizedQuery();
        Dataset dataset = factory.getFederationDataset();
        SageExecutionContext.configureDefault(ARQ.getContext());
        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            ResultSet results = qexec.execSelect();
            List<QuerySolution> solutions = new ArrayList<>();
            results.forEachRemaining(querySolution -> {
                System.out.println(querySolution);
                solutions.add(querySolution);
            });
            assertEquals("It should find 22 solutions bindings", 22, solutions.size());
        }
    }

    @Ignore
    @Test
    public void federatedQuery() {
        String url = "http://sage.univ-nantes.fr/sparql/dbpedia-2016-04";
        String queryString = "PREFIX dbp: <http://dbpedia.org/property/>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "\n" +
                "SELECT ?titleEng ?title\n" +
                "WHERE {\n" +
                "  ?movie dbp:starring [ rdfs:label 'Natalie Portman'@en ].\n" +
                "  SERVICE <http://sage.univ-nantes.fr/sparql/dbpedia-2015-04en> {\n" +
                "    ?movie rdfs:label ?titleEng, ?title.\n" +
                "  }\n" +
                "  FILTER LANGMATCHES(LANG(?titleEng), 'EN')\n" +
                "  FILTER (!LANGMATCHES(LANG(?title), 'EN'))\n" +
                "}\n";
        Query query = QueryFactory.create(queryString);
        FederatedQueryFactory factory = new ServiceFederatedQueryFactory(url, query);
        factory.buildFederation();
        query = factory.getLocalizedQuery();
        Dataset dataset = factory.getFederationDataset();
        SageExecutionContext.configureDefault(ARQ.getContext());
        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            ResultSet results = qexec.execSelect();
            List<QuerySolution> solutions = new ArrayList<>();
            results.forEachRemaining(querySolution -> {
                System.out.println(querySolution);
                solutions.add(querySolution);
            });
            assertEquals("It should find 218 solutions bindings", 218, solutions.size());
        }
    }

    @Ignore
    @Test
    public void describeQuery() {
        String url = "http://172.16.8.50:8000/sparql/bsbm1k";
        String queryString = "PREFIX rev: <http://purl.org/stuff/rev#>\n" +
                "DESCRIBE ?x\n" +
                "WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review4194> rev:reviewer ?x }";
        Query query = QueryFactory.create(queryString);
        FederatedQueryFactory factory = new ServiceFederatedQueryFactory(url, query);
        factory.buildFederation();
        query = factory.getLocalizedQuery();
        Dataset dataset = factory.getFederationDataset();
        SageExecutionContext.configureDefault(ARQ.getContext());
        QueryExecutor executor = new DescribeQueryExecutor("ttl");
        executor.execute(dataset, query);
    }
}
