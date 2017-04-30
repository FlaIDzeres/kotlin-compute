import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Created by flaidzeres on 30.04.17.
 */
public class App {
    /** Cmp. */
    public static final Compute cmp = new Compute();

    /** Java serializable path. */
    public static final String javaSerPath = "files/java/";

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        int val0 = 0;

        File dir = new File(javaSerPath);

        if (!dir.exists())
            dir.mkdirs();

        cmp.to("nodeId1", (SerializeRunnable)nodeId1 -> {
            int val1 = val0 + 1;

            System.out.println("Compute on " + nodeId1 + " val1=" + val1);

            cmp.to("nodeId2", (SerializeRunnable)nodeId2 -> {
                int val2 = val1 + 2;

                System.out.println("Compute on " + nodeId2 + " val1=" + val1 + " val2=" + val2);

                cmp.to("nodeId3", (SerializeRunnable)nodeId3 -> {
                    int val3 = val2 + 3;

                    System.out.println("Compute on " + nodeId3 + " val1=" + val1 + " val2=" + val2 + " val3=" + val3);

                    System.out.println("Compute result:" + val3);
                });
            });
        });
    }

    private interface SerializeRunnable extends Serializable {
        /**
         * @param nodeId Node id.
         */
        public void compute(String nodeId);
    }

    private static class Compute implements Serializable {
        /**
         * @param nodeId Node id.
         * @param run Run.
         */
        public void to(String nodeId, SerializeRunnable run) {
            try {
                System.out.println("Serialize callback");

                ObjectOutput out = new ObjectOutputStream(new FileOutputStream(javaSerPath + "callBack" + nodeId));

                out.writeObject(run);

                out.close();

                // Send to another node
                System.out.println("Send compute to " + nodeId);

                // Received callback on {nodeId}
                System.out.println("DeSerialize callback");

                ObjectInput in = new ObjectInputStream(new FileInputStream("files/java/callBack" + nodeId));

                App.SerializeRunnable run0 = (SerializeRunnable)in.readObject();

                in.close();

                run0.compute(nodeId);
            }
            catch (Throwable e) {
                System.out.println("Got exception:" + e);
            }
        }

        /**
         *
         */
        protected Object readResolve() {
            return cmp;
        }
    }
}

