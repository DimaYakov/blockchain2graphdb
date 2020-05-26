package ru.bmstu.yakov.blockchain2graph;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionGraph.class);

    protected static final String APP_NAME = "jgex";
    protected static final String MIXED_INDEX_CONFIG_NAME = "jgex";

    protected String propFileName;
    protected Configuration conf;
    protected Graph graph;
    protected GraphTraversalSource g;
    protected boolean supportsTransactions;
    protected boolean supportsSchema;

    protected boolean useMixedIndex;
    protected String mixedIndexConfigName;

    /**
     * Constructs a graph app using the given properties.
     * @param fileName location of the properties file
     */
    public TransactionGraph(final String fileName) {
        this.propFileName = fileName;
        this.supportsSchema = true;
        this.supportsTransactions = true;
        this.useMixedIndex = true;
        this.mixedIndexConfigName = MIXED_INDEX_CONFIG_NAME;
    }

    /**
     * Opens the graph instance. If the graph instance does not exist, a new
     * graph instance is initialized.
     */
    public GraphTraversalSource openGraph() throws ConfigurationException {
        LOGGER.info("opening graph");
        conf = new PropertiesConfiguration(propFileName);
        graph = GraphFactory.open(conf);
        g = graph.traversal();
        useMixedIndex = useMixedIndex && conf.containsKey("index." + mixedIndexConfigName + ".backend");
        return g;
    }

    /**
     * Closes the graph instance.
     */
    public void closeGraph() throws Exception {
        LOGGER.info("closing graph");
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

    /**
     * Returns the JanusGraph instance.
     */
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }

    /**
     * Drops the graph instance.
     */
    public void dropGraph() throws Exception {
        if (graph != null) {
            JanusGraphFactory.drop(getJanusGraph());
        }
    }

    /**
     * Creates the graph schema.
     */
    public void createSchema() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            // naive check if the schema was previously created
            if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
                management.rollback();
                return;
            }
            LOGGER.info("creating schema");
            createProperties(management);
            createVertexLabels(management);
            createEdgeLabels(management);
            createCompositeIndexes(management);
            createMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            management.rollback();
        }
    }

    /**
     * Creates the vertex labels.
     */
    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("Block").make();
        management.makeVertexLabel("Transaction").make();
        management.makeVertexLabel("Output").make();
        management.makeVertexLabel("Address").make();
    }

    /**
     * Creates the edge labels.
     */
    protected void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("chain").multiplicity(Multiplicity.ONE2ONE).make();
        management.makeEdgeLabel("has").multiplicity(Multiplicity.ONE2MANY).make();
        management.makeEdgeLabel("input").multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel("output").multiplicity(Multiplicity.MULTI).make();
        management.makeEdgeLabel("locked").multiplicity(Multiplicity.MULTI).make();
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
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

    /**
     * Creates the composite indexes. A composite index is best used for
     * exact match lookups.
     */
    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("nameIndex", Vertex.class).addKey(management.getPropertyKey("name")).buildCompositeIndex();
        management.buildIndex("blockIndex", Vertex.class).addKey(management.getPropertyKey("BlockHeight")).buildCompositeIndex();
    }

    /**
     * Creates the mixed indexes. A mixed index requires that an external
     * indexing backend is configured on the graph instance. A mixed index
     * is best for full text search, numerical range, and geospatial queries.
     */
    protected void createMixedIndexes(final JanusGraphManagement management) {
        if (useMixedIndex) {
            management.buildIndex("vAge", Vertex.class).addKey(management.getPropertyKey("age"))
                    .buildMixedIndex(mixedIndexConfigName);
            management.buildIndex("eReasonPlace", Edge.class).addKey(management.getPropertyKey("reason"))
                    .addKey(management.getPropertyKey("place")).buildMixedIndex(mixedIndexConfigName);
        }
    }

    public GraphTraversalSource initializeTransactionGraph() throws Exception {

        // open and initialize the graph
        GraphTraversalSource g = openGraph();

        // define the schema before loading data
        if (supportsSchema) {
            createSchema();
        }

        return g;
    }
}

