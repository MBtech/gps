package gps.examples.gm.avg_teen_cnt;
import gps.*;
import gps.graph.*;
import gps.node.*;
import gps.node.*;
import gps.writable.*;
import gps.globalobjects.*;
import org.apache.commons.cli.CommandLine;
import org.apache.mina.core.buffer.IoBuffer;
import java.io.IOException;
import java.io.BufferedWriter;
import java.util.Random;
import java.lang.Math;

public class avg_teen_cnt{

    // Keys for shared_variables 
    private static final String KEY_K = "K";
    private static final String KEY__cnt3 = "_cnt3";
    private static final String KEY__S2 = "_S2";

    public static class avg_teen_cntMaster extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public avg_teen_cntMaster (CommandLine line) {
            // parse command-line arguments (if any)
            java.util.HashMap<String,String> arg_map = new java.util.HashMap<String,String>();
            gps.node.Utils.parseOtherOptions(line, arg_map);

            if (arg_map.containsKey("K")) {
                String s = arg_map.get("K");
                K = Integer.parseInt(s);
            }
        }

        //save output
        public void writeOutput(BufferedWriter bw) throws IOException {
            bw.write("_ret_value:\t" + _ret_value + "\n");
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private int K;
        private int _cnt3;
        private int _S2;
        private float _ret_value; // the final return value of the procedure

        //----------------------------------------------------------
        // Master's State-machine 
        //----------------------------------------------------------
        private void _master_state_machine() {
            _master_should_start_workers = false;
            _master_should_finish = false;
            do {
                _master_state = _master_state_nxt ;
                switch(_master_state) {
                    case 0: _master_state_0(); break;
                    case 2: _master_state_2(); break;
                    case 3: _master_state_3(); break;
                    case 9: _master_state_9(); break;
                    case 5: _master_state_5(); break;
                }
            } while (!_master_should_start_workers && !_master_should_finish);

        }

        //@ Override
        public void compute(int superStepNo) {
            _master_state_machine();

            if (_master_should_finish) { 
                // stop the system 
                this.continueComputation = false;
                return;
            }

            if (_master_should_start_workers) { 
                 // start workers with state _master_state
            }
        }

        private void _master_state_0() {
            /*------
            _S2 = 0;
            _cnt3 = 0;
            -----*/
            System.out.println("Running _master_state 0");

            _S2 = 0 ;
            _cnt3 = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (n : G.Nodes)
            {
                n._S1prop = 0;
            }

            Foreach (t : G.Nodes)
                If (((t.age >= 10)  && (t.age < 20) ) )
                {
                    Foreach (n : t.Nbrs)
                        n._S1prop += 1 @ t ;
                }
            -----*/
            System.out.println("Running _master_state 2");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 3;
            _master_should_start_workers = true;
        }
        private void _master_state_3() {
            /*------
            -----*/
            System.out.println("Running _master_state 3");

            _master_state_nxt = 9;
        }
        private void _master_state_9() {
            /*------
            //Receive Nested Loop
            Foreach (n : t.Nbrs)
                n._S1prop += 1 @ t ;
            Foreach (n : G.Nodes)
            {
                n.teen_cnt = n._S1prop;
                If ((n.age > K) )
                {
                    _S2 += n.teen_cnt @ n ;
                    _cnt3 += 1 @ n ;
                }
            }
            -----*/
            System.out.println("Running _master_state 9");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_K,new IntOverwriteGlobalObject(K));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__cnt3,new IntSumGlobalObject(0));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__S2,new IntSumGlobalObject(0));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 5;
            _master_should_start_workers = true;
        }
        private void _master_state_5() {
            /*------
            _avg4 = (0 == _cnt3)  ? 0.000000 : (_S2 /  (Double ) _cnt3) ;
            avg =  (Float ) _avg4;
            Return avg;
            -----*/
            System.out.println("Running _master_state 5");
            _cnt3 = _cnt3+((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY__cnt3)).getValue().getValue();
            _S2 = _S2+((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY__S2)).getValue().getValue();
            float avg;
            double _avg4;

            _avg4 = (0 == _cnt3)?(float)(0.000000):(_S2 / ((double)_cnt3)) ;
            avg = (float)_avg4 ;
            _ret_value = avg;

            _master_should_finish = true;
        }
    }

    //----------------------------------------------
    // Main Vertex Class
    //----------------------------------------------
    public static class avg_teen_cntVertex
        extends NullEdgeVertex< avg_teen_cnt.VertexData, avg_teen_cnt.MessageData > {

        public avg_teen_cntVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData(getRandom().nextInt(20) + 10);
        }

        @Override
        public void compute(Iterable<avg_teen_cnt.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 9: _vertex_state_9(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<avg_teen_cnt.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (n : G.Nodes)
            {
                n._S1prop = 0;
            }

            Foreach (t : G.Nodes)
                If (((t.age >= 10)  && (t.age < 20) ) )
                {
                    Foreach (n : t.Nbrs)
                        n._S1prop += 1 @ t ;
                }
            -----*/

            {
                _this._S1prop = 0 ;
            }

            if (((_this.age >= 10) && (_this.age < 20)))
            {
                // Sending messages to all neighbors
                MessageData _msg = new MessageData((byte) 0);
                sendMessages(getNeighborIds(), _msg);
            }
        }
        private void _vertex_state_9(Iterable<avg_teen_cnt.MessageData> _msgs) {
            VertexData _this = getValue();
            int K = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_K)).getValue().getValue();

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                Foreach (n : t.Nbrs)
                    n._S1prop += 1 @ t ;
                -----*/
                _this._S1prop = _this._S1prop + (1);
            }

            /*------
            //Receive Nested Loop
            Foreach (n : t.Nbrs)
                n._S1prop += 1 @ t ;
            Foreach (n : G.Nodes)
            {
                n.teen_cnt = n._S1prop;
                If ((n.age > K) )
                {
                    _S2 += n.teen_cnt @ n ;
                    _cnt3 += 1 @ n ;
                }
            }
            -----*/

            {
                _this.teen_cnt = _this._S1prop ;
                if ((_this.age > K))
                {
                    getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__S2,new IntSumGlobalObject(_this.teen_cnt));
                    getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__cnt3,new IntSumGlobalObject(1));
                }
            }
        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class avg_teen_cntVertexFactory
        extends NullEdgeVertexFactory< avg_teen_cnt.VertexData, avg_teen_cnt.MessageData > {
        @Override
        public NullEdgeVertex< avg_teen_cnt.VertexData, avg_teen_cnt.MessageData > newInstance(CommandLine line) {
            return new avg_teen_cntVertex(line);
        }
    } // end of VertexFactory

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // properties
        int age;
        int teen_cnt;
        int _S1prop;

        public VertexData(int age) {
        	this.age = age;
        }
		@Override
        public int numBytes() {return 12;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(age);
            IOB.putInt(teen_cnt);
            IOB.putInt(_S1prop);
        }
        @Override
        public void read(IoBuffer IOB) {
            age= IOB.getInt();
            teen_cnt= IOB.getInt();
            _S1prop= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            age= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            teen_cnt= Utils.byteArrayToIntBigEndian(_BA, _idx + 4);
            _S1prop= Utils.byteArrayToIntBigEndian(_BA, _idx + 8);
            return 12;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 12);
            return 12;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "age: " + age + " " + "" + "teen_cnt: " + teen_cnt;
        }
        //Input Data Parsing
        @Override
        public void read(String inputString) {
            this.age = Integer.parseInt(inputString);
        }
    } // end of data class

    //----------------------------------------------
    // Message Data 
    //----------------------------------------------
    public static class MessageData extends MinaWritable {
        //single messge type; argument ignored
        public MessageData(byte type) {}


        public MessageData() {
            // default constructor that is required for constructing a representative instance for IncomingMessageStore.
        }
        // union of all message fields  

        @Override
        public int numBytes() {
            return 1; // empty message 
        }
        @Override
        public void write(IoBuffer IOB) {
            IOB.put((byte)0); // empty message
        }
        @Override
        public void read(IoBuffer IOB) {
            IOB.get(); // consume empty message byte
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            _idx++; // consume empty message byte
            return 1;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            //empty message(dummy byte)
            IOB.get(_BA, _idx+0, 1);
            return 1;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
            //do nothing
        }

    } // end of message-data


    // job description for the system
    public static class JobConfiguration extends GPSJobConfiguration {
        @Override
        public Class<?> getMasterClass() {
            return avg_teen_cntMaster.class;
        }
        @Override
        public Class<?> getVertexClass() {
            return avg_teen_cntVertex.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return avg_teen_cntVertexFactory.class;
        }
        @Override
        public Class<?> getVertexValueClass() {
            return VertexData.class;
        }
        @Override
        public Class<?> getEdgeValueClass() {
            return NullWritable.class;
        }
        @Override
        public Class<?> getMessageValueClass() {
            return MessageData.class;
        }
    }
}
