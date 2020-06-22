package ru.bmstu.yakov.blockchain2graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class BlockchainToGraph {
    private static String BLOCKSPATH;
    private static String BITCOINDPATH;
    private static GraphTraversalSource g;
    private static NetworkParameters np;
    private static TransactionGraph tg;
    private boolean isExit;
    private boolean canContinue;
    private int walletIDCounter;
    private String best;
    private int height;
    private int currentFileCount;
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockchainToGraph.class);
    private final int delay = 1126;
    private final int halfDelay = 100;

    //Get best chain off Graph Database
    private void getBestAndHeight() {
        LOGGER.info("Checking if blocks downloaded");
        if (g.V().has("Block", "BlockHeight", 0).hasNext()) {
            LOGGER.info("Block 0 is downloaded. Loading best chain. It can take more than 1 minute");
            canContinue = false;
            final Vertex block = g.V().has("Block", "BlockHeight", 0)
                    .repeat(__.out("chain")).until(__.not(__.out("chain"))).next();
            best = block.value("name");
            height = block.value("BlockHeight");
            LOGGER.info("Loaded best chain: best = " + best + "; height = " + height);
        }
    }

    public BlockchainToGraph(GraphTraversalSource g, NetworkParameters np,
                             TransactionGraph tg, String fileNameBitcoin, String fileNameBlockchainData) {
        this.g = g;
        this.np = np;
        this.tg = tg;
        this.isExit = false;
        this.canContinue = true;
        this.walletIDCounter = 0;
        this.BLOCKSPATH = fileNameBlockchainData;
        this.BITCOINDPATH = fileNameBitcoin;
        this.getBestAndHeight();
        this.getCurrentFile();
    }

    //Comparator for sorting blocks according to Date
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

    //Get last blk***.dat file
    private void getCurrentFile() {
        for (int i = 0; true; i++) {
            File file = new File(BLOCKSPATH + "/blocks/" + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())  {
                currentFileCount = i - 1;
                break;
            }
        }
    }

    //Adds new block to Graph Database and deletes wrong chain
    private void updateDatabase(String newHash, int newHeight) {

        if (height >= newHeight) {
            LOGGER.info("Invalid chain was found. Deleting invalid blocks");
            //Deleting wrong blocks
            while (height >= newHeight) {
                Vertex nextBlock = g.V().has("Block", "name", best).in("chain").next();
                deleteBlock(best, height);
                best = nextBlock.value("name");
                height--;
            }
        }

        //Add new block
        BlockFileLoader loader = new BlockFileLoader(np, buildList());

        for (Block blk : loader) {
            if (blk.getHashAsString().equals(newHash)) {
                parseBlock(blk, newHeight);
                break;
            }
        }
    }

    //Starting Synchronize blocks
    private void synchronizeDatabase() {
        LOGGER.info("Start synchronizing");
        String s;
        Process p;
        try {

            //Start bitcoin full node
            LOGGER.info("Starting full Bitcoin node");
            p = Runtime.getRuntime()
                    .exec(BITCOINDPATH +"/bitcoind -datadir=" + BLOCKSPATH);

            //Patterns to find in messages
            String patternFile = "Pre-allocating up to position";
            String patternBLK = "blk";
            String patternDat = ".dat";
            String patternChangeFile = "Leaving block file";
            String patternUpdateBlockChain = "UpdateTip: new best=";
            String patternBlockFileInfo = "CBlockFileInfo";
            String patternHeight = "height=";
            String patternVersion = "version=";

            //Get messages from node
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((s = br.readLine()) != null) {
                if (isExit) {
                    p.destroy();
                }
                if (s.contains(patternFile) && s.contains(patternBLK)) {
                    //if node choose blk***.dat file to write
                    currentFileCount = Integer.parseInt(s.substring(s.indexOf(patternBLK) + patternBLK.length(),
                            s.indexOf(patternDat)));
                } else if (s.contains(patternChangeFile)) {
                    //if node changes blk***.dat file to write
                    currentFileCount = Integer.parseInt(s.substring(s.indexOf(patternChangeFile) + patternChangeFile.length(),
                            s.indexOf(patternBlockFileInfo) - 2)) + 1;
                } else if (s.contains(patternUpdateBlockChain)) {
                    //if node finds ne block
                    String newHash = s.substring(s.indexOf(patternUpdateBlockChain) + patternUpdateBlockChain.length(),
                            s.indexOf(patternHeight) - 1);
                    int newHeight = Integer.parseInt(s.substring(s.indexOf(patternHeight) + patternHeight.length(),
                            s.indexOf(patternVersion) - 1));

                    LOGGER.info("New block was found with hash " + newHash);

                    //Call update method to add new block and delete wrong chain
                    updateDatabase(newHash, newHeight);
                }
            }
            p.waitFor();
            p.destroy();
        } catch (Exception e) {}
    }

    //This methods adds Block to Graph Database
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

    //This methods adds Transaction to Graph Database
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

    //This methods updates output into input of Graph Database
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

    //This methods adds Input to Graph Database
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

    //This methods adds Address to Graph Database
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

    //This method parses blockchain. It parses blk***.dat files stored on disk and
    //adds information from them in Graph Database
    public void parseBlockChain() throws Exception{

        //Creating a BlockFileLoader object by passing a list of files.
        BlockFileLoader loader = new BlockFileLoader(np, new File(BLOCKSPATH + "/blocks/"));

        //Counter to control the progress
        int blockCounter = 0;
        int sortedBlockCounter = 0;
        int parsedBlockCounter = 0;

        //List of blocks to sort them in right ay and parse them
        List<Block> blockList = new ArrayList<Block>();

        //Hash of previous block to check the blockchain
        String previousHash = "";
        //Hash of previous block to sort block files from blockList
        String prevHash = "";
        //Flag of the first loop
        boolean firstLoop = true;

        //Parsing all blockchain
        for (Block blk : loader) {

            if (isExit) {
                tg.closeGraph();
                LOGGER.info("Last checked block " + (blockCounter - 1) + ". Shutting down");
                System.exit(0);
            }
            //Adding blocks to be sorted
            blockList.add(blk);
            sortedBlockCounter++;
            if (sortedBlockCounter == delay) {

                //Sorting Blocks
                for (int i = 0; i <= halfDelay; i++) {
                    if (firstLoop) {
                        //Hash of the genesis Block
                        previousHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
                        prevHash = previousHash;
                        firstLoop = false;
                    } else {
                        for (int j = 0; j < blockList.size(); j++) {
                            Block b = blockList.get(i);
                            String curHash = b.getPrevBlockHash().toString();
                            if (!curHash.equals(prevHash)) {
                                blockList.remove(i);
                                blockList.add(b);
                            } else {
                                prevHash = b.getHashAsString();
                                break;
                            }
                        }
                    }
                }
                prevHash = blockList.get(halfDelay-1).getHashAsString();

                //Parsing sorted blocks
                for (Block block : blockList) {

                    //Delete parsed blocks from blockList
                    if (parsedBlockCounter == halfDelay)  {
                        parsedBlockCounter = 0;
                        sortedBlockCounter = delay - halfDelay;
                        ArrayList<Block> newBlockList = new ArrayList<Block>(blockList.subList(halfDelay, delay));
                        blockList.clear();
                        blockList = newBlockList;
                        break;
                    }

                    //Checking sorted blocks for right chain
                    if (blockCounter > 0) {
                        String currentHash = block.getPrevBlockHash().toString();
                        if (!currentHash.equals(previousHash)) {
                            throw new Exception("Invalid chain");
                        } else {
                            previousHash = block.getHashAsString();
                        }
                    }

                    //Parsing block
                    if (isExit) {
                        tg.closeGraph();
                        LOGGER.info("Last checked block " + (blockCounter - 1) + ". Shutting down");
                        System.exit(0);
                    }
                    LOGGER.info("Analysing block "+blockCounter);
                    if (canContinue) {
                        parseBlock(block, blockCounter);
                    } else {
                        if (block.getHashAsString().equals(best)) {
                            canContinue = true;
                        } else if (!g.V().has("Block", "name", block.getHashAsString()).hasNext()) {
                            String lastBlockHash = block.getPrevBlockHash().toString();
                            deleteInvalidBlocks(lastBlockHash);
                            canContinue = true;
                            parseBlock(block, blockCounter);
                        } else {
                            LOGGER.info("Block "+blockCounter+" is already in database");
                        }
                    }
                    blockCounter++;
                    parsedBlockCounter++;
                }
            }

        }

        //Sorting last files
        Collections.sort(blockList, blockComparator);

        for (int i = 0; i < blockList.size() - 1; i++) {
            for (int j = 0; j < blockList.size(); j++) {
                Block b = blockList.get(i);
                String curHash = b.getPrevBlockHash().toString();
                if (!curHash.equals(prevHash)) {
                    blockList.remove(i);
                    blockList.add(b);
                } else {
                    prevHash = b.getHashAsString();
                    break;
                }
            }
        }

        //Checking sorted blocks for right chain
        for (Block block : blockList) {

            if (blockCounter > 0) {
                String currentHash = block.getPrevBlockHash().toString();
                if (!currentHash.equals(previousHash)) {
                    throw new Exception("Invalid chain");
                } else {
                    previousHash = block.getHashAsString();
                }
            }

            //Parsing block
            if (isExit) {
                tg.closeGraph();
                LOGGER.info("Last checked block " + (blockCounter - 1) + ". Shutting down");
                System.exit(0);
            }
            LOGGER.info("Analysing block "+blockCounter);
            if (canContinue) {
                parseBlock(block, blockCounter);
            } else {
                String checkBlockHash = block.getHashAsString();
                if (checkBlockHash.equals(best)) {
                    canContinue = true;
                }  else if (!g.V().has("Block", "name", checkBlockHash).hasNext()) {
                    String lastBlockHash = block.getPrevBlockHash().toString();
                    deleteInvalidBlocks(lastBlockHash);
                    canContinue = true;
                    parseBlock(block, blockCounter);
                } else {
                    LOGGER.info("Block "+blockCounter+" is already in database");
                }
            }
            blockCounter++;
            parsedBlockCounter++;
        }

    }

    //Deleting invalid blocks
    private void deleteInvalidBlocks(String lastBlockHash) {
        while (!lastBlockHash.equals(best)) {
            Vertex nextBlock = g.V().has("Block", "name", best).in("chain").next();
            deleteBlock(best, height);
            best = nextBlock.value("name");
            height--;
        }
    }

    //Calculating blocks signs, then calling addBlockToGraph method
    private void addBlock(Block block, int blockCounter) {

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

    //Recalculating blocks signs, then updating block in Graph Database
    private void updateBlock(String blockHash, int blockCounter, long blockBalance, long blockFee) {
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

    //Recalculating output signs when transaction is deleted,
    //then calling updating Output and it's Address in Graph Database
    private void updateOutput(Vertex output) {
        try {

            String o = output.value("name");

            LOGGER.info("Updating input " + o + "into output");

            g.V().has("name", o).property("OutputIsUsed", false).iterate();
            g.tx().commit();

            final Vertex address = g.V().has("name", o).out("locked").next();
            String addressAddress = address.value("name");

            Date addressFirstAppearDate = address.value("AddressFirstAppearDate");
            Date addressLastAppearDate = addressFirstAppearDate;
            List<Vertex> transactions = g.V().has("name", addressAddress).in("locked").in("output").toList();
            for (Vertex tx : transactions) {
                Date date = tx.value("TransactionDate");
                if (date.after(addressLastAppearDate)) {
                    addressLastAppearDate = date;
                }
            }
            LOGGER.info("Updating address " + addressAddress);

            long addressBalance = address.value("AddressBalance");
            long outputBalance = output.value("OutputBalance");
            addressBalance += outputBalance;
            long addressInputTransactionBalance = address.value("AddressInputTransactionBalance");
            long addressOutputTransactionBalance = address.value("AddressOutputTransactionBalance");
            addressOutputTransactionBalance -= outputBalance;
            int addressTransactionCount = address.value("AddressTransactionCount");
            addressTransactionCount--;
            int addressInputTransactionCount = address.value("AddressInputTransactionCount");
            int addressOutputTransactionCount = address.value("AddressOutputTransactionCount");
            addressOutputTransactionCount--;
            int addressInputAddressCount = address.value("AddressInputAddressCount");
            int addressOutputAddressCount = address.value("AddressOutputAddressCount");
            int addressBetweenWalletTransactionCount = address.value("AddressBetweenAddressTransactionCount");
            int addressWalletID = address.value("AddressWalletID");

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

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();
        }
    }

    //Deleting output from Graph Database
    private void deleteOutput(String transactionHash, Vertex output, int blockCounter) {
        try {
            String o = output.value("name");

            final Vertex address = g.V().has("name", o).out("locked").next();
            String addressAddress = address.value("name");

            Date addressFirstAppearDate = address.value("AddressFirstAppearDate");
            Date addressLastAppearDate = address.value("AddressLastAppearDate");

            if (!addressFirstAppearDate.before(addressLastAppearDate)) {
                LOGGER.info("Deleting address " + addressAddress);

                g.V().has("name", addressAddress).drop().iterate();

                g.tx().commit();

            } else {
                LOGGER.info("Updating address " + addressAddress);
                long outputBalance = output.value("OutputBalance");
                long addressBalance = address.value("AddressBalance");
                addressBalance -= outputBalance;

                List<Vertex> transactions = g.V().has("name", addressAddress).in("locked").in("output").toList();

                addressLastAppearDate = addressFirstAppearDate;
                for (Vertex tx : transactions) {
                    Date date = tx.value("TransactionDate");
                    if (date.after(addressLastAppearDate)) {
                        addressLastAppearDate = date;
                    }
                }

                long addressInputTransactionBalance = address.value("AddressInputTransactionBalance");
                addressInputTransactionBalance -= outputBalance;
                long addressOutputTransactionBalance = address.value("AddressOutputTransactionBalance");
                int addressTransactionCount = address.value("AddressTransactionCount");
                addressTransactionCount--;
                int addressInputTransactionCount = address.value("AddressInputTransactionCount");
                addressInputTransactionCount--;
                int addressOutputTransactionCount = address.value("AddressOutputTransactionCount");
                int addressInputAddressCount = address.value("AddressInputAddressCount");
                int addressOutputAddressCount = address.value("AddressOutputAddressCount");
                int addressBetweenWalletTransactionCount = address.value("AddressBetweenAddressTransactionCount");
                int addressWalletID = address.value("AddressWalletID");

                boolean inputAddress = g.V().has("name", transactionHash)
                        .in("input").out("locked").has("name", addressAddress).hasNext();

                if (inputAddress) {
                    addressBetweenWalletTransactionCount--;
                }

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
            }

            LOGGER.info("Deleting output " + o);
            g.V().has("name", o).drop().iterate();

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();
        }
    }

    //Deleting Transaction from Graph Database
    private void deleteTransaction(Vertex transaction, int blockCounter) {
        try {

            String tx = transaction.value("name");
            LOGGER.info("Deleting transaction " + tx + " from block " + blockCounter);

            final List<Vertex> outputs = g.V().has("name", tx).out("output").toList();
            final List<Vertex> inputs = g.V().has("name", tx).in("input").toList();

            Set<String> addresses = new HashSet<>();

            for (Vertex in : inputs) {
                String s = in.value("name");
                String address = g.V().has("name", s).out("locked").next().value("name");
                addresses.add(address);
            }
            for (Vertex out : outputs) {
                String s = out.value("name");
                String address = g.V().has("name", s).out("locked").next().value("name");
                addresses.add(address);
            }

            for (Vertex o : outputs) {
                deleteOutput(tx, o, blockCounter);
            }

            for (Vertex in : inputs) {
                updateOutput(in);
            }

            g.V().has("name", tx).drop().iterate();

            g.tx().commit();

            for (String address : addresses) {
                calculateAndUpdateAddress(address);
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();
        }
    }

    //Deleting Block from Graph Database
    private void deleteBlock(String blockHash, int blockCounter) {
        try {
            LOGGER.info("Deleting block " + blockCounter);

            final List<Vertex> transactions = g.V().has("name", blockHash).out("has").toList();

            for (Vertex tx : transactions) {
                deleteTransaction(tx, blockCounter);
            }
            g.V().has("name", blockHash).drop().iterate();

            g.tx().commit();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            g.tx().rollback();
        }
    }

    //Calculating transaction signs, then calling addTransactionToGraph method
    private void addTransaction(String blockHash, int blockHeight, int transactionInputCount, int transactionOutputCount,
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

        List <Vertex> ins = g.V().has("name", transactionHash).in("input").toList();
        List <Vertex> outs = g.V().has("name", transactionHash).out("output").toList();
        Set<String> addresses = new HashSet<>();

        for (Vertex in : ins) {
            String s = in.value("name");
            String address = g.V().has("name", s).out("locked").next().value("name");
            addresses.add(address);
        }
        for (Vertex out : outs) {
            String s = out.value("name");
            String address = g.V().has("name", s).out("locked").next().value("name");
            addresses.add(address);
        }
        for (String address : addresses) {
            calculateAndUpdateAddress(address);
        }
    }

    //Recalculating transaction signs, then updating transaction in Graph Database
    private void updateTransaction(String transactionHash, int transactionNewAddressCount) {
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

    //Calculating address signs, then calling addAddressToGraph method
    private void addAddress(String transactionHash, String outputHash, String addressAddress, long outputBalance, Date date) {

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
            addressInputAddressCount = address.value("AddressInputAddressCount");
            addressOutputAddressCount = address.value("AddressOutputAddressCount");
            addressBetweenWalletTransactionCount = address.value("AddressBetweenAddressTransactionCount");
            addressWalletID = address.value("AddressWalletID");

            boolean inputAddress = g.V().has("name", transactionHash)
                    .in("input").out("locked").has("name", addressAddress).hasNext();

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

    //Recalculating address signs, then updating address in GraphDatabase
    private void updateAddress(String outputHash, Date date) {

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

    //Recalculating complex address signs, then updating Address in Graph Database
    private void calculateAndUpdateAddress(String addressAddress) {
        if (g.V().has("name", addressAddress).hasNext()) {
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
                    if (!a.equals(addressAddress)) outAddresses.add(a);
                }
            }

            g.V().has("name", addressAddress).property("AddressInputAddressCount", inAddresses.size())
                    .property("AddressOutputAddressCount", outAddresses.size()).next();

            g.tx().commit();
        }
    }

    //Calling addInputToGraph method and updating connected address
    private void addInput(String transactionHash, String connectedOutputTransactionHash, int connectedOutputHeight, Date date) {

        String outputHash = connectedOutputTransactionHash + ":" + connectedOutputHeight;

        addInputToGraph(transactionHash, outputHash);

        updateAddress(outputHash, date);

    }

    //Calling addOutputToGraph method and updating connected address
    private void addOutput(int blockCounter, String transactionHash, String outputHash, int outputHeight,
                   long outputValue, String address, Date date) {
        boolean outputIsUsed = false;

        addOutputToGraph(blockCounter, transactionHash, outputHash, outputHeight, outputValue, outputIsUsed);

        addAddress(transactionHash, outputHash, address, outputValue, date);
    }

    //Parsing block then adding it in Graph Database
    private void parseBlock(Block block, int blockCounter) {

        //Get some data from block
        Date date = block.getTime();
        long blockBalance = block.getBlockInflation(blockCounter).longValue();
        long blockFee = 0;

        //Calling method to add block to Graph Database
        addBlock(block, blockCounter);

        //Check if block has transactions
        if (block.hasTransactions()) {

            //Loop over transaction of Block
            for (Transaction tx : block.getTransactions()) {

                //Get transaction hash
                String txHash = tx.getHashAsString();

                //Get inputs and outputs of transaction
                List<TransactionInput> txInputs = tx.getInputs();
                List<TransactionOutput> txOutputs = tx.getOutputs();

                //Calling method to add transaction to Graph Database
                addTransaction(block.getHashAsString(), blockCounter, txInputs.size(), txOutputs.size(), tx, date);

                //Check if transaction is not coin base
                if (!tx.isCoinBase()) {

                    //Get some data from transaction
                    blockBalance += tx.getInputSum().longValue();
                    Coin fee = tx.getFee();
                    if (fee != null) {
                        blockFee += fee.longValue();
                    }

                    //Loop over inputs
                    for (TransactionInput ti : txInputs) {

                        //Get some data from input
                        TransactionOutPoint to = ti.getOutpoint();
                        String tHash = to.getHash().toString();
                        int index = (int) to.getIndex();

                        //Calling method to add Input to Graph Database
                        addInput(txHash, tHash, index, date);
                    }
                }

                //Counter of new addresses in transaction
                int transactionNewAddressCount = 0;

                //Loop over outputs
                for (TransactionOutput to : txOutputs) {

                    //Get some data from output
                    Coin value = to.getValue();
                    int id = to.getIndex();

                    //Get address of output
                    String ad = "";
                    try {
                        ad = to.getScriptPubKey().getToAddress(np, true).toString();
                    } catch (final ScriptException x) {
                        ad = "Невозможно декодировать выходной адрес";
                    } catch (final IllegalArgumentException x) {
                        ad = "Невозможно декодировать выходной адрес";
                    }

                    //Count new addresses in transaction
                    final boolean isOldAddress = g.V().has("name", ad).hasNext();
                    if (!isOldAddress) {
                        transactionNewAddressCount++;
                    }
                    String outputHash = txHash + ":" + id;

                    //Calling method to add Output to Graph Database
                    addOutput(blockCounter, txHash, outputHash, id, value.longValue(), ad, date);
                }

                //Calling method to update transaction in Graph Database
                updateTransaction(txHash, transactionNewAddressCount);
            }
        }

        //Calling method to update block in Graph Database
        updateBlock(block.getHashAsString(), blockCounter, blockBalance, blockFee);
    }

    // Return a list of files in a directory method with blk***.dat format
    private List<File> buildList() {
        List<File> list = new LinkedList<File>();
        for (int i = currentFileCount; true; i++) {
            File file = new File(BLOCKSPATH + "/blocks/" + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())
                break;
            list.add(file);
        }

        return list;
    }

    //Main method that initialise Graph and starts parsing
    public static void main(String[] args) throws Exception {

        String fileNameConfig = "";
        String fileNameBlockchainData = "";
        String fileNameBitcoin = "";

        if (args != null && args.length > 0 && args.length != 1) {
            LOGGER.info("Invalid number of arguments = " + args.length +
                    ". It can be only 1 argument with filepath to blockchain2graph.conf file");
            System.exit(0);
        } else {
            File file;
            if (args == null || args.length == 0) {
                String path = System.getProperty("user.dir");
                file = new File( path + "/blockchain2graph.conf");
                if (!file.exists()) {
                    LOGGER.info("There's no config file blockchain2graph.conf in current path");
                    System.exit(0);
                }
            } else {
                String path = args[0];
                file = new File(path);
                if (!file.exists()) {
                    LOGGER.info("There's no path " + path);
                    System.exit(0);
                }
                file = new File( path + "/blockchain2graph.conf");
                if (!file.exists()) {
                    LOGGER.info("There's no config file blockchain2graph.conf in path " + path);
                    System.exit(0);
                }
            }
            List<String> paths = new ArrayList<String>();
            FileReader fr = new FileReader(file);
            BufferedReader reader = new BufferedReader(fr);
            String line = reader.readLine();
            while (line != null) {
                paths.add(line);
                line = reader.readLine();
            }
            if (paths.size() != 3) {
                LOGGER.info("Invalid format of blockchain2graph.conf file."
                        + " It must contain 3 lines with with datadir=, configdir= and bitcoindir= on lines with arguments");
                System.exit(0);
            } else {
                for (String path : paths) {
                    String dataDir = "datadir=";
                    String configDir = "configdir=";
                    String bitcoinDir = "bitcoindir=";
                    if (path.contains(dataDir)) {
                        fileNameBlockchainData = path.substring(path.indexOf(dataDir) + dataDir.length());
                    } else if (path.contains(configDir)) {
                        fileNameConfig = path.substring(path.indexOf(configDir) + configDir.length());
                    } else if (path.contains(bitcoinDir)) {
                        fileNameBitcoin = path.substring(path.indexOf(bitcoinDir) + bitcoinDir.length());
                    }
                }
                if (fileNameBitcoin.equals("")) {
                    LOGGER.info("There's no bitcoindir= argument in blockchain2graph.conf or null argument");
                    System.exit(0);
                } else if (fileNameBlockchainData.equals("")) {
                    LOGGER.info("There's no datadir= argument in blockchain2graph.conf or null argument");
                    System.exit(0);
                } else if (fileNameConfig.equals("")) {
                    LOGGER.info("There's no configdir= argument in blockchain2graph.conf");
                    System.exit(0);
                }

                String p;
                p = fileNameBitcoin;
                file = new File(p);
                if (!file.exists()) {
                    LOGGER.info("There's no path " + p + " from blockchain2graph.conf bitcoindir= argument");
                    System.exit(0);
                }
                p = fileNameBlockchainData;
                file = new File(p);
                if (!file.exists()) {
                    LOGGER.info("There's no path " + p + " from blockchain2graph.conf datadir= argument");
                    System.exit(0);
                }
                p = fileNameConfig;
                file = new File(p);
                if (!file.exists()) {
                    LOGGER.info("There's no path " + p + " from blockchain2graph.conf configdir= argument");
                    System.exit(0);
                }

                file = new File(fileNameBitcoin + "/bitcoind");
                if (!file.exists()) {
                    LOGGER.info("There's no bitcoind file in path " + p
                            + " from blockchain2graph.conf bitcoindir= argument");
                    System.exit(0);
                }
                file = new File(fileNameConfig+ "/blk-cql.properties");
                if (!file.exists()) {
                    LOGGER.info("There's no blk-cql.properties file in path " + p
                            + " from blockchain2graph.conf configdir= argument");
                    System.exit(0);
                }
                file = new File(fileNameBlockchainData+ "/blocks");
                if (!file.exists()) {
                    LOGGER.info("There's no blocks directory in path " + p
                            + " from blockchain2graph.conf datadir= argument");
                    System.exit(0);
                }
            }
        }

        TransactionGraph tg = new TransactionGraph(fileNameConfig+ "/blk-cql.properties");

        //Some initial setup
        NetworkParameters np = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());
        GraphTraversalSource g = tg.openGraph();

        //Initialize block parser
        BlockchainToGraph bp = new BlockchainToGraph(g, np, tg, fileNameBitcoin, fileNameBlockchainData);

        //Initialize transaction graph database
        if (bp.canContinue) {
            tg.g = tg.initializeTransactionGraph();
        }

        //Handle of CTRL+C event to end last block parsing
        SignalHandler sh = new SignalHandler() {
            public void handle(Signal signal) {
                bp.isExit = true;
            }
        };
        Signal.handle(new Signal("INT"), sh);

        //Parse already downloaded blockchain
        bp.parseBlockChain();

        //Start synchronizing blockchain
        bp.synchronizeDatabase();

        /*JanusGraph graph = tg.getJanusGraph();
        graph.io(IoCore.graphml()).writeGraph("output/export.xml");*/

        tg.closeGraph();
        System.exit(0);
        //tg.dropGraph();
    }
}
