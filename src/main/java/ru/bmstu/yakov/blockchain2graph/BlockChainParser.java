package ru.bmstu.yakov.blockchain2graph;

import java.io.File;
import java.text.SimpleDateFormat;
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

    // A simple method with everything in it
    public void doSomething() {

        // Just some initial setup
        NetworkParameters np = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());

        // We create a BlockFileLoader object by passing a list of files.
        // The list of files is built with the method buildList(), see
        // below for its definition.
        BlockFileLoader loader = new BlockFileLoader(np,new File(PREFIX));

        // We are going to store the results in a map of the form
        // day -> n. of transactions
        Map<String, Integer> dailyTotTxs = new HashMap();

        // A simple counter to have an idea of the progress
        int blockCounter = 0;

        // bitcoinj does all the magic: from the list of files in the loader
        // it builds a list of blocks. We iterate over it using the following
        // for loop
        Date prev = null;
        for (Block block : loader) {

            //if (blockCounter > 100000) break;
            blockCounter++;
            // This gives you an idea of the progress
            System.out.println("Analysing block "+blockCounter);

            // Extract the day from the block: we are only interested
            // in the day, not in the time. Block.getTime() returns
            // a Date, which is here converted to a string.
            String day = new SimpleDateFormat("yyyy-MM-dd").format(block.getTime());

            // Now we start populating the map day -> number of transactions.
            // Is this the first time we see the date? If yes, create an entry
            if (!dailyTotTxs.containsKey(day)) {
                dailyTotTxs.put(day, 0);
            }

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
        } // End of iteration over blocks

    }  // end of doSomething() method.

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
