package ru.bmstu.yakov.blockchain2graph;

import javafx.util.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.BlockStore;
import org.bitcoinj.utils.BlockFileLoader;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

public class GraphDatabaseUpdater {

    GraphTraversalSource g;
    static String PREFIX = "/home/chuits/snap/bitcoin-core/common/.bitcoin/blocks/";
    int currentFileCount;

    String best;
    int height;
    NetworkParameters np;

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphDatabaseUpdater.class);

    public GraphDatabaseUpdater(GraphTraversalSource g, NetworkParameters np) {
        this.g = g;
        this.np = np;
        this.getBestAndHeight();
        this.getCurrentFile();
    }

    void getCurrentFile() {
        for (int i = 0; true; i++) {
            File file = new File(PREFIX + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())  {
                currentFileCount = i - 1;
                break;
            }
        }
    }

    void getBestAndHeight() {
        final Vertex block = g.V().has("Block", "BlockHeight", 0)
                .repeat(__.out("chain")).until(__.not(__.out("chain"))).next();
        best = block.value("name");
        height = block.value("BlockHeight");
        LOGGER.info("Loaded best chain: best = " + best + "; height = " + height);
    }

    void updateDatabase(String newHash, int newHeight, BlockChainParser bp) {

        while (height >= newHeight) {
            Vertex nextBlock = g.V().has("Block", "name", best).in("chain").next();
            bp.deleteBlock(best, height);
            best = nextBlock.value("name");
            height--;
        }

        BlockFileLoader loader = new BlockFileLoader(np, buildList());

        for (Block blk : loader) {
            if (blk.getHashAsString().equals(newHash)) {
                bp.parseBlock(blk, newHeight);
                break;
            }
        }

    }

    void synchronizeDatabase(BlockChainParser bp) {
        String s;
        Process p;
        try {
            p = Runtime.getRuntime()
                    .exec("/snap/bitcoin-core/63/bin/bitcoind -datadir=/home/chuits/snap/bitcoin-core/common/.bitcoin");

            String patternFile = "Pre-allocating up to position";
            String patternBLK = "blk";
            String patternDat = ".dat";
            String patternChangeFile = "Leaving block file";
            String patternUpdateBlockChain = "UpdateTip: new best=";
            String patternBlockFileInfo = "CBlockFileInfo";
            String patternHeight = "height=";
            String patternVersion = "version=";
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((s = br.readLine()) != null) {
                if (s.contains(patternFile) && s.contains(patternBLK)) {
                    currentFileCount = Integer.parseInt(s.substring(s.indexOf(patternBLK) + patternBLK.length(),
                            s.indexOf(patternDat)));
                } else if (s.contains(patternChangeFile)) {
                    currentFileCount = Integer.parseInt(s.substring(s.indexOf(patternChangeFile) + patternChangeFile.length(),
                            s.indexOf(patternBlockFileInfo) - 2)) + 1;
                } else if (s.contains(patternUpdateBlockChain)) {
                    String newHash = s.substring(s.indexOf(patternUpdateBlockChain) + patternUpdateBlockChain.length(),
                            s.indexOf(patternHeight) - 1);
                    int newHeight = Integer.parseInt(s.substring(s.indexOf(patternHeight) + patternHeight.length(),
                            s.indexOf(patternVersion) - 1));

                    updateDatabase(newHash, newHeight, bp);
                }
            }
            p.waitFor();
            System.out.println ("exit: " + p.exitValue());
            p.destroy();
        } catch (Exception e) {}
    }

    // Main method: simply invoke everything
    public static void main(String[] args) throws Exception {

        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        TransactionGraph tg = new TransactionGraph(fileName);

        GraphTraversalSource g = tg.openGraph();


        NetworkParameters np = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());

        BlockChainParser bp = new BlockChainParser(g, np);
        GraphDatabaseUpdater gdu = new GraphDatabaseUpdater(g, np);

        //du.synchronizeDatabase(bp);

        //tg.closeGraph();
        tg.dropGraph();
    }

    private List<File> buildList() {
        List<File> list = new LinkedList<File>();
        for (int i = currentFileCount; true; i++) {
            System.out.println(i);
            File file = new File(PREFIX + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())
                break;
            list.add(file);
        }

        return list;
    }
}
