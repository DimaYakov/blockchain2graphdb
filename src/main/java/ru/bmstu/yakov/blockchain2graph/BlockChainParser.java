package ru.bmstu.yakov.blockchain2graph;

import java.io.File;
import java.util.*;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockChainParser {
    // Location of block files. This is where your blocks are located.
    // Check the documentation of Bitcoin Core if you are using
    // it, or use any other directory with blk*dat files.
    private static GraphTraversalSource g;
    private static NetworkParameters np;
    private int walletIDCounter;
    static String PREFIX = "/home/chuits/snap/bitcoin-core/common/.bitcoin/blocks/";
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockChainParser.class);
    int delay = 1125;
    final int halfDelay = 100;

    public BlockChainParser(GraphTraversalSource g, NetworkParameters np) {
        this.g = g;
        this.np = np;
        this.walletIDCounter = 0;
    }

    Comparator blockComparator = new Comparator<Block>()
    {
        public int compare(Block a, Block b)
        {
            Date aDate = a.getTime();
            Date bDate = b.getTime();
            if (aDate.before(bDate)) {
                return -1;
            } else return 1;
        }

    };

    private void addBlockToGraph(String prevBlockHash, String curBlockHash, Date blockDate,
                                        int blockHeight, int blockTransactionCount, long blockBalance,
                                        long blockCoinBaseBalance, long blockFee) {

        try {
            // naive check if the graph was previously created
            if (g.V().has("name", curBlockHash).hasNext()) {
                g.tx().rollback();
                return;
            }
            LOGGER.info("Adding new block " + blockHeight);
            final Vertex curBlock = g.addV("Block").property("name", curBlockHash)
                    .property("BlockDate", blockDate).property("BlockHeight", blockHeight)
                    .property("BlockTransactionCount", blockTransactionCount).property("BlockBalance", blockBalance)
                    .property("BlockCoinBaseBalance", blockCoinBaseBalance).property("BlockFee", blockFee).next();

            g.tx().commit();

            if (blockHeight != 0) {
                final Vertex prevBlock = g.V().has("name", prevBlockHash).next();
                g.V(prevBlock).as("a").V(curBlock).addE("chain").from("a").next();
            }

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();

        }

    }

    private void addTransactionToGraph(String blockHash, int blockHeight, String transactionHash,
                                              int transactionInputCount, int transactionOutputCount,
                                       long transactionBalance, Date transactionDate, int transactionNewAddressCount,
                                       boolean transactionIsCoinBase, long transactionFee,
                                       boolean transactionIsBetweenOneAddress) {
        try {
            // naive check if the graph was previously created
            if (g.V().has("name", transactionHash).hasNext()) {
                g.tx().rollback();
                return;
            }
            LOGGER.info("Adding new transaction " + transactionHash + " from block " + blockHeight);

            final Vertex transaction = g.addV("Transaction").property("name", transactionHash)
                    .property("TransactionInputCount", transactionInputCount)
                    .property("TransactionOutputCount", transactionOutputCount)
                    .property("TransactionBalance", transactionBalance)
                    .property("TransactionDate", transactionDate)
                    .property("TransactionNewAddressCount", transactionNewAddressCount)
                    .property("TransactionIsCoinBase", transactionIsCoinBase)
                    .property("TransactionFee", transactionFee)
                    .property("TransactionIsBetweenAddress", transactionIsBetweenOneAddress).next();

            final Vertex block = g.V().has("name", blockHash).next();

            g.V(block).as("a").V(transaction).addE("has").from("a").next();

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();

        }
    }

    private void addInputToGraph(String transactionHash, String outputHash) {
        try {
            LOGGER.info("Updating output " + outputHash + " into input");

            g.V().has("name", outputHash).property("OutputIsUsed", true).iterate();

            final Vertex input = g.V().has("name", outputHash).next();
            final Vertex transaction = g.V().has("name", transactionHash).next();

            g.V(input).as("a").V(transaction).addE("input").from("a").next();

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();
        }
    }

    private void addOutputToGraph(int blockHeight, String transactionHash,
                                  String outputHash, int outputHeight, long outputBalance, boolean outputIsUsed) {
        try {
            // naive check if the graph was previously created
            if (g.V().has("name", outputHash).hasNext()) {
                g.tx().rollback();
                return;
            }
            LOGGER.info("Adding new output " + outputHash+ " from transaction "
                    + transactionHash + " from block " + blockHeight);

            final Vertex output = g.addV("Output").property("name", outputHash)
                    .property("OutputHeight", outputHeight).property("OutputBalance", outputBalance)
                    .property("OutputIsUsed", outputIsUsed).next();

            g.tx().commit();

            final Vertex transaction = g.V().has("name", transactionHash).next();

            g.V(transaction).as("a").V(output).addE("output").from("a").next();

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();

        }
    }

    private void addAddressToGraph(String outputHash, String addressAddress, long addressBalance,
                                   Date addressFirstAppearDate, Date addressLastAppearDate,
                                   long addressInputTransactionBalance, long addressOutputTransactionBalance,
                                   int addressTransactionCount,
                                   int addressInputTransactionCount, int addressOutputTransactionCount,
                                   int addressInputAddressCount, int addressOutputAddressCount,
                                   int addressBetweenWalletTransactionCount, int addressWalletID) {
        try {

            Vertex address;
            // naive check if the graph was previously created
            if (g.V().has("name", addressAddress).hasNext()) {
                LOGGER.info("Updating address " + addressAddress);
                g.tx().rollback();

                g.V().has("name", addressAddress)
                        .property("AddressBalance", addressBalance)
                        .property("AddressFirstAppearDate", addressFirstAppearDate)
                        .property("AddressLastAppearDate", addressLastAppearDate)
                        .property("AddressInputTransactionBalance", addressInputTransactionBalance)
                        .property("AddressOutputTransactionBalance", addressOutputTransactionBalance)
                        .property("AddressTransactionCount", addressTransactionCount)
                        .property("AddressInputTransactionCount", addressInputTransactionCount)
                        .property("AddressOutputTransactionCount", addressOutputTransactionCount)
                        .property("AddressInputAddressCount", addressInputAddressCount)
                        .property("AddressOutputAddressCount", addressOutputAddressCount)
                        .property("AddressBetweenAddressTransactionCount", addressBetweenWalletTransactionCount)
                        .property("AddressWalletID", addressWalletID).iterate();
                address = g.V().has("name", addressAddress).next();
            } else {
                LOGGER.info("Adding new address " + addressAddress);

                address = g.addV("Address").property("name", addressAddress)
                        .property("AddressBalance", addressBalance)
                        .property("AddressFirstAppearDate", addressFirstAppearDate)
                        .property("AddressLastAppearDate", addressLastAppearDate)
                        .property("AddressInputTransactionBalance", addressInputTransactionBalance)
                        .property("AddressOutputTransactionBalance", addressOutputTransactionBalance)
                        .property("AddressTransactionCount", addressTransactionCount)
                        .property("AddressInputTransactionCount", addressInputTransactionCount)
                        .property("AddressOutputTransactionCount", addressOutputTransactionCount)
                        .property("AddressInputAddressCount", addressInputAddressCount)
                        .property("AddressOutputAddressCount", addressOutputAddressCount)
                        .property("AddressBetweenAddressTransactionCount", addressBetweenWalletTransactionCount)
                        .property("AddressWalletID", addressWalletID).next();
            }
            final Vertex output = g.V().has("name", outputHash).next();

            g.V(output).as("a").V(address).addE("locked").from("a").next();

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();

        }
    }

    // A simple method with everything in it
    public void parseBlockChain() {

        // We create a BlockFileLoader object by passing a list of files.
        // The list of files is built with the method buildList(), see
        // below for its definition.
        BlockFileLoader loader = new BlockFileLoader(np, new File(PREFIX));
        //BlockFileLoader loader = new BlockFileLoader(np,buildList());

        // A simple counter to have an idea of the progress
        int blockCounter = 0;
        int sortedBlockCounter = 0;
        int parsedBlockCounter = 0;
        List<Block> blockList = new ArrayList<Block>();

        // bitcoinj does all the magic: from the list of files in the loader
        // it builds a list of blocks. We iterate over it using the following
        // for loop
        //Date prev = null;

        for (Block blk : loader) {
            if (blockCounter == 2500) break;
            blockList.add(blk);
            sortedBlockCounter++;
            System.out.println("Block "+ (blockCounter + sortedBlockCounter) + " added to be sorted");
            if (sortedBlockCounter == delay) {
                System.out.println("Sorting blocks from " + blockCounter + " to " + (blockCounter + delay));
                Collections.sort(blockList, blockComparator);

                String prevHash = blockList.get(0).getHashAsString();

                for (int i = 1; i < blockList.size() - 1; i++) {
                    Block b = blockList.get(i);
                    String curHash = b.getPrevBlockHash().toString();
                    if (!curHash.equals(prevHash)) {
                        blockList.remove(i);
                        blockList.add(i+1, b);
                        prevHash = blockList.get(i).getHashAsString();
                    } else {
                        prevHash = b.getHashAsString();
                    }
                }

                System.out.println("Blocks are sorted");
                for (Block block : blockList) {

                    if (parsedBlockCounter == halfDelay)  {
                        parsedBlockCounter = 0;
                        sortedBlockCounter = delay - halfDelay;
                        ArrayList<Block> newBlockList = new ArrayList<Block>(blockList.subList(halfDelay, delay));
                        blockList.clear();
                        blockList = newBlockList;
                        break;
                    }

                    if (blockCounter == 2500) break;
                    System.out.println("Analysing block "+blockCounter);

                    /*Date cur = block.getTime();
                    if (prev == null) {
                        prev = cur;
                    } else {
                        if (prev.after(cur)) {
                            System.out.println("Blocks are not sorted");
                            System.out.println("previous BlockDate " + prev);
                            System.out.println("current BlockDate " + cur);
                            break;
                        } else {
                            prev = cur;
                        }
                    }*/
                    parseBlock(block, blockCounter);

                    blockCounter++;
                    parsedBlockCounter++;
                }
            }

        } // End of iteration over blocks

        /*System.out.println("Sorting blocks from " + blockCounter + " to " + (blockCounter + delay));
        Collections.sort(blockList, blockComparator);

        String prevHash = blockList.get(0).getHashAsString();

        for (int i = 1; i < blockList.size() - 1; i++) {
            Block b = blockList.get(i);
            String curHash = b.getPrevBlockHash().toString();
            if (!curHash.equals(prevHash)) {
                blockList.remove(i);
                blockList.add(i+1, b);
                prevHash = blockList.get(i).getHashAsString();
            } else {
                prevHash = b.getHashAsString();
            }
        }

        System.out.println("Blocks are sorted");

        for (Block block : blockList) {

            System.out.println("Analysing block "+blockCounter);
            parseBlock(block, blockCounter);

            blockCounter++;
            parsedBlockCounter++;
        }*/

    }  // end of doSomething() method.

    void addBlock(Block block, int blockCounter) {

        String curBlockHash = block.getHashAsString();
        Date blockDate = block.getTime();
        int blockTransactionCount = 0;
        if (block.hasTransactions()) {
            blockTransactionCount = block.getTransactions().size();
        }
        long blockCoinBaseBalance = block.getBlockInflation(blockCounter).longValue();
        long blockBalance = blockCoinBaseBalance;
        long blockFee = 0;

        addBlockToGraph(block.getPrevBlockHash().toString(), curBlockHash, blockDate, blockCounter, blockTransactionCount,
                blockBalance, blockCoinBaseBalance, blockFee);
    }

    void updateBlock(String blockHash, int blockCounter, long blockBalance, long blockFee) {
        try {
            LOGGER.info("Updating block " + blockCounter);

            g.V().has("name", blockHash)
                    .property("BlockBalance", blockBalance).property("BlockFee", blockFee).iterate();

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();
        }
    }

    void addTransaction(String blockHash, int blockHeight, int transactionInputCount, int transactionOutputCount,
                        Transaction transaction, Date transactionDate) {

        String transactionHash = transaction.getHashAsString();
        long transactionBalance = transaction.getInputSum().longValue();
        int transactionNewAddressCount = 0;
        boolean transactionIsCoinBase = transaction.isCoinBase();
        long transactionFee;
        Coin fee = transaction.getFee();
        if (fee == null) {
            transactionFee = 0;
        } else {
            transactionFee = fee.longValue();
        }
        boolean transactionIsBetweenOneAddress = false;

        addTransactionToGraph(blockHash, blockHeight, transactionHash, transactionInputCount,
                transactionOutputCount, transactionBalance, transactionDate, transactionNewAddressCount,
                transactionIsCoinBase, transactionFee, transactionIsBetweenOneAddress);
    }

    void updateTransaction(String transactionHash, int transactionNewAddressCount) {
        try {
            LOGGER.info("Updating transaction " + transactionHash);

            g.V().has("name", transactionHash)
                    .property("TransactionNewAddressCount", transactionNewAddressCount).iterate();

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();
        }
    }

    void addAddress(String transactionHash, String outputHash, String addressAddress, long outputBalance, Date date) {

        long addressBalance;
        Date addressFirstAppearDate;
        Date addressLastAppearDate;
        long addressInputTransactionBalance;
        long addressOutputTransactionBalance;
        int addressTransactionCount;
        int addressInputTransactionCount;
        int addressOutputTransactionCount;
        int addressInputAddressCount;
        int addressOutputAddressCount;
        int addressBetweenWalletTransactionCount;
        int addressWalletID;

        if (g.V().has("name", addressAddress).hasNext()) {
            g.tx().rollback();

            final Vertex address = g.V().has("name", addressAddress).next();
            addressBalance = address.value("AddressBalance");
            addressBalance += outputBalance;
            addressFirstAppearDate = address.value("AddressFirstAppearDate");
            addressLastAppearDate = date;
            addressInputTransactionBalance = address.value("AddressInputTransactionBalance");
            addressInputTransactionBalance += outputBalance;
            addressOutputTransactionBalance = address.value("AddressOutputTransactionBalance");
            addressTransactionCount = address.value("AddressTransactionCount");
            addressTransactionCount++;
            addressInputTransactionCount = address.value("AddressInputTransactionCount");
            addressInputTransactionCount++;
            addressOutputTransactionCount = address.value("AddressOutputTransactionCount");
            //????????????????
            addressInputAddressCount = address.value("AddressInputAddressCount");
            addressOutputAddressCount = address.value("AddressOutputAddressCount");
            addressBetweenWalletTransactionCount = address.value("AddressBetweenAddressTransactionCount");
            addressWalletID = address.value("AddressWalletID");

            //final List<Vertex> ins = g.V().has("name", transactionHash).in("input").toList();

            boolean inputAddress = g.V().has("name", transactionHash)
                    .in("input").out("locked").has("name", addressAddress).hasNext();

            /*for (Vertex in : ins) {
                String name = in.value("name");
                if (g.V().has("name", name).out("locked").has("name", addressAddress).hasNext()) {
                    inputAddress = true;
                    break;
                }
            }*/

            if (inputAddress) {
                addressBetweenWalletTransactionCount++;

                g.V().has("name", transactionHash)
                        .property("TransactionIsBetweenOneAddress", true).iterate();
                g.tx().commit();
            }

        } else {
            addressBalance = outputBalance;
            addressFirstAppearDate = date;
            addressLastAppearDate = date;
            addressInputTransactionBalance = outputBalance;
            addressOutputTransactionBalance = 0;
            addressTransactionCount = 1;
            addressInputTransactionCount = 1;
            addressOutputTransactionCount = 0;
            //????????????????
            addressInputAddressCount = 0;
            addressOutputAddressCount = 0;
            addressBetweenWalletTransactionCount = 0;
            addressWalletID = 0;
        }

        addAddressToGraph(outputHash, addressAddress, addressBalance, addressFirstAppearDate, addressLastAppearDate,
                addressInputTransactionBalance,  addressOutputTransactionBalance, addressTransactionCount,
                addressInputTransactionCount,  addressOutputTransactionCount, addressInputAddressCount,
                addressOutputAddressCount, addressBetweenWalletTransactionCount,  addressWalletID);
    }

    void updateAddress(String outputHash, Date date) {

        final Vertex address = g.V().has("name", outputHash).out("locked").next();
        final Vertex output = g.V().has("name", outputHash).next();

        String addressAddress = address.value("name");
        long addressBalance = address.value("AddressBalance");
        long outputBalance = output.value("OutputBalance");
        addressBalance -= outputBalance;
        Date addressFirstAppearDate = address.value("AddressFirstAppearDate");;
        Date addressLastAppearDate = date;
        long addressInputTransactionBalance = address.value("AddressInputTransactionBalance");
        long addressOutputTransactionBalance = address.value("AddressOutputTransactionBalance");
        addressOutputTransactionBalance += outputBalance;
        int addressTransactionCount = address.value("AddressTransactionCount");
        addressTransactionCount++;
        int addressInputTransactionCount = address.value("AddressInputTransactionCount");
        int addressOutputTransactionCount = address.value("AddressOutputTransactionCount");
        addressOutputTransactionCount++;
        //?????????????????????????
        int addressInputAddressCount = address.value("AddressInputAddressCount");
        int addressOutputAddressCount = address.value("AddressOutputAddressCount");
        int addressBetweenWalletTransactionCount = address.value("AddressBetweenAddressTransactionCount");
        int addressWalletID = address.value("AddressWalletID");

        LOGGER.info("Updating address " + addressAddress);

        g.V().has("name", addressAddress)
                .property("AddressBalance", addressBalance)
                .property("AddressFirstAppearDate", addressFirstAppearDate)
                .property("AddressLastAppearDate", addressLastAppearDate)
                .property("AddressInputTransactionBalance", addressInputTransactionBalance)
                .property("AddressOutputTransactionBalance", addressOutputTransactionBalance)
                .property("AddressTransactionCount", addressTransactionCount)
                .property("AddressInputTransactionCount", addressInputTransactionCount)
                .property("AddressOutputTransactionCount", addressOutputTransactionCount)
                .property("AddressInputAddressCount", addressInputAddressCount)
                .property("AddressOutputAddressCount", addressOutputAddressCount)
                .property("AddressBetweenAddressTransactionCount", addressBetweenWalletTransactionCount)
                .property("AddressWalletID", addressWalletID).iterate();

        g.tx().commit();
    }

    void calculateAndUpdateAddress(String addressAddress) {

        final List<Vertex> inTransactions = g.V().has("name", addressAddress).in("locked").in("output").toList();
        final List<Vertex> outTransactions = g.V().has("name", addressAddress).in("locked").out("input").toList();

        Set<String> inAddresses = new HashSet<>();
        Set<String> outAddresses = new HashSet<>();

        for (Vertex in : inTransactions) {
            String name = in.value("name");
            final List<Vertex> inAddressList = g.V().has("name", name).in("input").toList();
            for (Vertex o : inAddressList) {
                String hash = o.value("name");
                String a = g.V().has("name", hash).out("locked").next().value("name");
                if (!a.equals(addressAddress)) inAddresses.add(a);
            }
        }

        for (Vertex out : outTransactions) {
            String name = out.value("name");
            final List<Vertex> outAddressList = g.V().has("name", name).out("output").toList();
            for (Vertex o : outAddressList) {
                String hash = o.value("name");
                String a = g.V().has("name", hash).out("locked").next().value("name");
                if (!a.equals(addressAddress))outAddresses.add(a);
            }
        }

        g.V().has("name", addressAddress).property("AddressInputAddressCount", inAddresses.size())
                .property("AddressOutputAddressCount", outAddresses.size()).next();

        g.tx().commit();
    }

    void addInput(String transactionHash, String connectedOutputTransactionHash, int connectedOutputHeight, Date date) {

        String outputHash = connectedOutputTransactionHash + ":" + connectedOutputHeight;

        addInputToGraph(transactionHash, outputHash);

        updateAddress(outputHash, date);

        final Vertex address = g.V().has("name", outputHash).out("locked").next();
        String addressAddress = address.value("name");

        calculateAndUpdateAddress(addressAddress);
    }

    void addOutput(int blockCounter, String transactionHash, String outputHash, int outputHeight,
                   long outputValue, String address, Date date) {
        boolean outputIsUsed = false;

        addOutputToGraph(blockCounter, transactionHash, outputHash, outputHeight, outputValue, outputIsUsed);

        addAddress(transactionHash, outputHash, address, outputValue, date);

        calculateAndUpdateAddress(address);
    }

    void parseBlock(Block block, int blockCounter) {
        // The following is highly inefficient: we could simply do
        // block.getTransactions().size(), but is shows you
        // how to iterate over transactions in a block
        // So, we simply iterate over all transactions in the
        // block and for each of them we add 1 to the corresponding
        // entry in the map
        Date date = block.getTime();

        addBlock(block, blockCounter);
        long blockBalance = block.getBlockInflation(blockCounter).longValue();
        long blockFee = 0;

        if (block.hasTransactions()) {
            for (Transaction tx : block.getTransactions()) {

                System.out.println("------------------------------------------------------");
                String txHash = tx.getHashAsString();
                System.out.println("TransactionHash " + txHash);

                List<TransactionInput> txInputs = tx.getInputs();
                List<TransactionOutput> txOutputs = tx.getOutputs();

                addTransaction(block.getHashAsString(), blockCounter, txInputs.size(), txOutputs.size(), tx, date);

                //System.out.println(tx.toString());
                if (!tx.isCoinBase()) {

                    blockBalance += tx.getInputSum().longValue();
                    Coin fee = tx.getFee();
                    if (fee != null) {
                        blockFee += fee.longValue();
                    }

                    for (TransactionInput ti : txInputs) {
                        TransactionOutPoint to = ti.getOutpoint();
                        String tHash = to.getHash().toString();
                        int index = (int) to.getIndex();

                        System.out.println("input  [" + tHash + ":" + index + "] ");

                        addInput(txHash, tHash, index, date);
                        //создать связь connectedOutputHash->txHash
                    }
                }

                int transactionNewAddressCount = 0;

                for (TransactionOutput to : txOutputs) {
                    Coin value = to.getValue();
                    int id = to.getIndex();
                    String ad = "";
                    try {
                        ad = to.getScriptPubKey().getToAddress(np, true).toString();
                    } catch (final ScriptException x) {
                        ad = "Невозможно декодировать выходной адрес";
                    } catch (final IllegalArgumentException x) {
                        ad = "Невозможно декодировать выходной адрес";
                    }

                    final boolean isOldAddress = g.V().has("name", ad).hasNext();
                    if (!isOldAddress) {
                        transactionNewAddressCount++;
                    }
                    String outputHash = txHash + ":" + id;
                    //создать ноду с адресом и количеством битков + связь outputHash->Address
                    System.out.println("output [" + outputHash + "] "+ value.toFriendlyString() + " to " + ad);

                    addOutput(blockCounter, txHash, outputHash, id, value.longValue(), ad, date);
                    //System.out.println(to.toString());
                }

                updateTransaction(txHash, transactionNewAddressCount);
            }
        }

        updateBlock(block.getHashAsString(), blockCounter, blockBalance, blockFee);

        System.out.println("------------------------------------------------------");
        System.out.println();
    }

    // The method returns a list of files in a directory according to a certain
    // pattern (block files have name blkNNNNN.dat)
    private List<File> buildList() {
        List<File> list = new LinkedList<File>();
        for (int i = 0; true; i++) {
            System.out.println(i);
            File file = new File(PREFIX + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())
                break;
            list.add(file);
        }

        return list;
    }

    // Main method: simply invoke everything
    public static void main(String[] args) throws Exception {

        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        TransactionGraph tg = new TransactionGraph(fileName);

        GraphTraversalSource g = tg.initializeTransactionGraph();

        // Just some initial setup
        NetworkParameters np = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());

        BlockChainParser bp = new BlockChainParser(g, np);
        bp.parseBlockChain();

        JanusGraph graph = tg.getJanusGraph();
        graph.io(IoCore.graphml()).writeGraph("output/export.xml");

        tg.dropGraph();
    }
}
