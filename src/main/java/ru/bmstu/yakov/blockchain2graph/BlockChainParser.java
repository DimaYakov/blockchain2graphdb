package ru.bmstu.yakov.blockchain2graph;

import java.io.File;
import java.util.*;

import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;

public class BlockChainParser {
    // Location of block files. This is where your blocks are located.
    // Check the documentation of Bitcoin Core if you are using
    // it, or use any other directory with blk*dat files.
    static String PREFIX = "/home/chuits/snap/bitcoin-core/common/.bitcoin/blocks/";
    final int delay = 2050;
    final int halfDelay = delay / 2;

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

    // A simple method with everything in it
    public void doSomething() {

        // Just some initial setup
        NetworkParameters np = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());

        // We create a BlockFileLoader object by passing a list of files.
        // The list of files is built with the method buildList(), see
        // below for its definition.
        BlockFileLoader loader = new BlockFileLoader(np,new File(PREFIX));

        // A simple counter to have an idea of the progress
        int blockCounter = 0;
        int sortedBlockCounter = 0;
        int parsedBlockCounter = 0;
        List<Block> blockList = new LinkedList<Block>();

        // bitcoinj does all the magic: from the list of files in the loader
        // it builds a list of blocks. We iterate over it using the following
        // for loop
        Date prev = null;
        for (Block blk : loader) {
            if (blockCounter > 2051) break;
            blockList.add(blk);
            sortedBlockCounter++;
            System.out.println("Block "+ (blockCounter + sortedBlockCounter) + " added to be sorted");
            if (sortedBlockCounter == delay) {
                System.out.println("Sorting blocks from " + blockCounter + " to " + (blockCounter + delay));
                Collections.sort(blockList, blockComparator);
                System.out.println("Blocks are sorted");
                for (Block block : blockList) {

                    if (parsedBlockCounter == halfDelay)  {
                        parsedBlockCounter = 0;
                        sortedBlockCounter = halfDelay;
                        LinkedList<Block> newBlockList = new LinkedList<Block>(blockList.subList(halfDelay, delay));
                        blockList.clear();
                        blockList = newBlockList;
                        break;
                    }

                    System.out.println("Analysing block "+blockCounter);
                    blockCounter++;
                    if (blockCounter > 2051) break;
                    parsedBlockCounter++;

                    Date cur = block.getTime();
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
                    }
                    parseBlock(block);
                }
            }

        } // End of iteration over blocks

    }  // end of doSomething() method.

    void parseBlock(Block block) {
        // The following is highly inefficient: we could simply do
        // block.getTransactions().size(), but is shows you
        // how to iterate over transactions in a block
        // So, we simply iterate over all transactions in the
        // block and for each of them we add 1 to the corresponding
        // entry in the map
        if (block.hasTransactions()) {
            for (Transaction tx : block.getTransactions()) {
                System.out.println("------------------------------------------------------");
                String txHash = tx.getHash().toString();
                System.out.println("TransactionHash " + txHash);

                List<TransactionInput> txInputs = tx.getInputs();
                List<TransactionOutput> txOutputs = tx.getOutputs();
                //System.out.println(tx.toString());
                if (!tx.isCoinBase()) {
                    for (TransactionInput ti : txInputs) {
                        TransactionOutPoint to = ti.getOutpoint();
                        String tHash = to.getHash().toString();
                        int index = (int) to.getIndex();

                        System.out.println("input  [" + tHash + ":" + index + "] ");
                        //создать связь connectedOutputHash->txHash
                    }
                }
                for (TransactionOutput to : txOutputs) {
                    Coin value = to.getValue();
                    int id = to.getIndex();
                    String ad;
                    try {
                        ad = to.getScriptPubKey().getToAddress(MainNetParams.get(), true).toString();
                    } catch (final ScriptException x) {
                        ad = "Невозможно декодировать выходной адрес";
                    } catch (final IllegalArgumentException x) {
                        ad = "Невозможно декодировать выходной адрес";
                    }
                    //String outputAddress = to.get.getHash().toString();
                    //создать ноду с адресом и количеством битков + связь outputHash->Address
                    System.out.println("output [" + txHash + ":" + id + "] "+ value.toFriendlyString() + " to " + ad);
                    //System.out.println(to.toString());
                }
            }
        }
        System.out.println("------------------------------------------------------");
        System.out.println();
    }

    // The method returns a list of files in a directory according to a certain
    // pattern (block files have name blkNNNNN.dat)
    private List<File> buildList() {
        List<File> list = new LinkedList<File>();
        for (int i = 0; true; i++) {
            File file = new File(PREFIX + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())
                break;
            list.add(file);
        }

        return list;
    }


    // Main method: simply invoke everything
    public static void main(String[] args) {
        BlockChainParser bp = new BlockChainParser();
        bp.doSomething();
    }
}
