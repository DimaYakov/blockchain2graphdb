package ru.bmstu.yakov.blockchain2graph;

import java.util.Date;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionGraph.class);

    protected String propFileName;
    protected Configuration conf;
    protected Graph graph;
    protected GraphTraversalSource g;


    //Construct a graph app using the given properties from fileName.
    public TransactionGraph(final String fileName) {
        this.propFileName = fileName;
    }

    //Opens the graph instance
    public GraphTraversalSource openGraph() throws ConfigurationException {
        LOGGER.info("Opening graph");
        conf = new PropertiesConfiguration(propFileName);
        graph = GraphFactory.open(conf);
        g = graph.traversal();
        return g;
    }

    //Close the graph
    public void closeGraph() throws Exception {
        LOGGER.info("Closing graph");

        try {
            if (g != null) {
                g.close();
            }
            if (graph != null) {
                graph.close();
            }
        } finally {
            g = null;
            graph = null;
        }
    }

    //Return the JanusGraph instance.
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }

    //Drop the graph
    public void dropGraph() throws Exception {
        if (graph != null) {
            JanusGraphFactory.drop(getJanusGraph());
        }
    }

    //Creates the graph schema.
    public void createSchema() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            // naive check if the schema was previously created
            if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
                management.rollback();
                return;
            }
            LOGGER.info("Creating schema");

            createProperties(management);
            createVertexLabels(management);
            createEdgeLabels(management);
            createCompositeIndexes(management);

            management.commit();

        } catch (Exception e) {
            management.rollback();
        }
    }

    //Creates the vertex labels.
    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("Block").make();
        management.makeVertexLabel("Transaction").make();
        management.makeVertexLabel("Output").make();
        management.makeVertexLabel("Address").make();
    }


    //Create the edge labels.
    protected void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("chain").multiplicity(Multiplicity.ONE2ONE).make();
        management.makeEdgeLabel("has").multiplicity(Multiplicity.ONE2MANY).make();
        management.makeEdgeLabel("input").multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel("output").multiplicity(Multiplicity.MULTI).make();
        management.makeEdgeLabel("locked").multiplicity(Multiplicity.MULTI).make();
    }


    //Create the properties for vertices
    protected void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("name").dataType(String.class).make();
        management.makePropertyKey("BlockDate").dataType(Date.class).make();
        management.makePropertyKey("BlockHeight").dataType(Integer.class).make();
        management.makePropertyKey("BlockTransactionCount").dataType(Integer.class).make();
        management.makePropertyKey("BlockBalance").dataType(Long.class).make();
        management.makePropertyKey("BlockCoinBaseBalance").dataType(Long.class).make();
        management.makePropertyKey("BlockFee").dataType(Integer.class).make();


        management.makePropertyKey("AddressBalance").dataType(Long.class).make();
        management.makePropertyKey("AddressFirstAppearDate").dataType(Date.class).make();
        management.makePropertyKey("AddressLastAppearDate").dataType(Date.class).make();
        management.makePropertyKey("AddressInputTransactionBalance").dataType(Long.class).make();
        management.makePropertyKey("AddressOutputTransactionBalance").dataType(Long.class).make();
        management.makePropertyKey("AddressTransactionCount").dataType(Integer.class).make();
        management.makePropertyKey("AddressInputTransactionCount").dataType(Integer.class).make();
        management.makePropertyKey("AddressOutputTransactionCount").dataType(Integer.class).make();
        management.makePropertyKey("AddressInputAddressCount").dataType(Integer.class).make();
        management.makePropertyKey("AddressOutputAddressCount").dataType(Integer.class).make();
        management.makePropertyKey("AddressBetweenAddressTransactionCount").dataType(Integer.class).make();
        management.makePropertyKey("AddressWalletID").dataType(Integer.class).make();


        management.makePropertyKey("TransactionInputCount").dataType(Integer.class).make();
        management.makePropertyKey("TransactionOutputCount").dataType(Integer.class).make();
        management.makePropertyKey("TransactionBalance").dataType(Long.class).make();
        management.makePropertyKey("TransactionDate").dataType(Date.class).make();
        management.makePropertyKey("TransactionNewAddressCount").dataType(Integer.class).make();
        management.makePropertyKey("TransactionIsCoinBase").dataType(Boolean.class).make();
        management.makePropertyKey("TransactionFee").dataType(Long.class).make();
        management.makePropertyKey("TransactionIsBetweenOneAddress").dataType(Boolean.class).make();


        management.makePropertyKey("OutputHeight").dataType(Integer.class).make();
        management.makePropertyKey("OutputBalance").dataType(Long.class).make();
        management.makePropertyKey("OutputIsUsed").dataType(Boolean.class).make();
    }


     //Creates the composite indexes
    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("nameIndex", Vertex.class).addKey(management.getPropertyKey("name")).buildCompositeIndex();
        management.buildIndex("blockIndex", Vertex.class).addKey(management.getPropertyKey("BlockHeight")).buildCompositeIndex();
    }

    public GraphTraversalSource initializeTransactionGraph() throws Exception {

        //Open the graph
        GraphTraversalSource g = openGraph();

        //Define the schema
        createSchema();

        return g;
    }
}

