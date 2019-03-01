package gps.examples.gm.pagerank_optimized;
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

public class pagerank{

    // Keys for shared_variables 
//	private static final String KEY_d = "d";
//    private static final String KEY_diff = "diff";
    private static final String KEY_N = "N";

    public static class pagerankMaster extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public pagerankMaster (CommandLine line) {
            // parse command-line arguments (if any)
            java.util.HashMap<String,String> arg_map = new java.util.HashMap<String,String>();
            gps.node.Utils.parseOtherOptions(line, arg_map);
//
//            if (arg_map.containsKey("e")) {
//                String s = arg_map.get("e");
//                e = Double.parseDouble(s);
//            }
            if (arg_map.containsKey("d")) {
                String s = arg_map.get("d");
                d = Double.parseDouble(s);
            }
            if (arg_map.containsKey("max")) {
                String s = arg_map.get("max");
                max = Integer.parseInt(s);
            }
        }

        //save output
        public void writeOutput(BufferedWriter bw) throws IOException {
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
//        private double e;
        private double d;
        private int max;
//        private double diff;
        private int cnt;
        private double N;

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
                    case 8: _master_state_8(); break;
                    case 9: _master_state_9(); break;
                    case 10: _master_state_10(); break;
                    case 16: _master_state_16(); break;
                    case 12: _master_state_12(); break;
                    case 4: _master_state_4(); break;
                    case 7: _master_state_7(); break;
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
            cnt = 0;
            N =  (Double ) G.NumNodes();
            -----*/
            System.out.println("Running _master_state 0");
            cnt = 0 ;
            N = (double)(getGraphSize()) ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (t0 : G.Nodes)
                t0.pg_rank = 1 / N;
            -----*/
            System.out.println("Running _master_state 2");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_N,new DoubleOverwriteGlobalObject(N));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 3;
            _master_should_start_workers = true;
        }
        private void _master_state_3() {
            /*------
            -----*/
            System.out.println("Running _master_state 3");
            _master_state_nxt = 8;
        }
        private void _master_state_8() {
            /*------
            diff = 0.000000;
            -----*/
            System.out.println("Running _master_state 8");
//            diff = (float)(0.000000) ;
            _master_state_nxt = 9;
        }
        private void _master_state_9() {
            /*------
            Foreach (t : G.Nodes)
            {
                t._S1prop = 0.000000;
            }

            Foreach (w : G.Nodes)
                If ((w.OutDegree() > 0) )
                {
                    Foreach (t : w.Nbrs)
                        t._S1prop += (w.pg_rank /  (Double ) w.OutDegree())  @ w ;
                }
            -----*/
            System.out.println("Running _master_state 9");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 10;
            _master_should_start_workers = true;
        }
        private void _master_state_10() {
            /*------
            -----*/
            System.out.println("Running _master_state 10");
            _master_state_nxt = 16;
        }
        private void _master_state_16() {
            /*------
            //Receive Nested Loop
            Foreach (t : w.Nbrs)
                t._S1prop += (w.pg_rank /  (Double ) w.OutDegree())  @ w ;
            Foreach (t : G.Nodes)
            {
                val = (1 - d)  / N + d * t._S1prop;
                diff +=  | (val - t.pg_rank)  |  @ t ;
                t.pg_rank_nxt = val;
                t.pg_rank = t.pg_rank_nxt;
            }
            -----*/
            System.out.println("Running _master_state 16");
            getGlobalObjectsMap().clearNonDefaultObjects();
//            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_d,new DoubleOverwriteGlobalObject(d));
//            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_diff,new DoubleSumGlobalObject(0));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_N,new DoubleOverwriteGlobalObject(N));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 12;
            _master_should_start_workers = true;
        }
        private void _master_state_12() {
            /*------
            cnt = cnt + 1;
            -----*/
            System.out.println("Running _master_state 12");
//            diff = diff+((DoubleSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY_diff)).getValue().getValue();
            cnt = cnt + 1 ;
            _master_state_nxt = 4;
        }
        private void _master_state_4() {
            /*------
            (diff > e)  && (cnt < max) 
            -----*/
            System.out.println("Running _master_state 4");
            // Do-While(...)

            boolean _expression_result = (cnt < max);
            if (_expression_result) _master_state_nxt = 8;
            else _master_state_nxt = 7;

        }
        private void _master_state_7() {
            /*------
            -----*/
            System.out.println("Running _master_state 7");

            _master_should_finish = true;
        }
    }

    //----------------------------------------------
    // Main Vertex Class
    //----------------------------------------------
    public static class pagerankVertex
        extends NullEdgeVertex< pagerank.VertexData, pagerank.MessageData > {

        public pagerankVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }

        @Override
        public void compute(Iterable<pagerank.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 9: _vertex_state_9(_msgs); break;
                case 16: _vertex_state_16(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<pagerank.MessageData> _msgs) {
            VertexData _this = getValue();
            double N = ((DoubleOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_N)).getValue().getValue();
            /*------
            Foreach (t0 : G.Nodes)
                t0.pg_rank = 1 / N;
            -----*/

            _this.pg_rank = 1 / N ;
        }
        private void _vertex_state_9(Iterable<pagerank.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t : G.Nodes)
            {
                t._S1prop = 0.000000;
            }

            Foreach (w : G.Nodes)
                If ((w.OutDegree() > 0) )
                {
                    Foreach (t : w.Nbrs)
                        t._S1prop += (w.pg_rank /  (Double ) w.OutDegree())  @ w ;
                }
            -----*/

            {
                _this._S1prop = (float)(0.000000) ;
            }

            if ((getNeighborsSize() > 0))
            {
                // Sending messages to all neighbors
                MessageData _msg = new MessageData((byte) 0);
                _msg.d0 = _this.pg_rank;
                sendMessages(getNeighborIds(), _msg);
            }
        }
        private void _vertex_state_16(Iterable<pagerank.MessageData> _msgs) {
            VertexData _this = getValue();
            double d = 0.85;
//            double d = ((DoubleOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_d)).getValue().getValue();
            double N = ((DoubleOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_N)).getValue().getValue();
            double val;

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                Foreach (t : w.Nbrs)
                    t._S1prop += (w.pg_rank /  (Double ) w.OutDegree())  @ w ;
                -----*/
                double _remote_pg_rank = _msg.d0;
                _this._S1prop = _this._S1prop + ((_remote_pg_rank / ((double)(getNeighborsSize()))));
            }

            /*------
            //Receive Nested Loop
            Foreach (t : w.Nbrs)
                t._S1prop += (w.pg_rank /  (Double ) w.OutDegree())  @ w ;
            Foreach (t : G.Nodes)
            {
                val = (1 - d)  / N + d * t._S1prop;
                diff +=  | (val - t.pg_rank)  |  @ t ;
                t.pg_rank_nxt = val;
                t.pg_rank = t.pg_rank_nxt;
            }
            -----*/

            {
                val = (1 - d) / N + d * _this._S1prop ;
//                getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_diff,new DoubleSumGlobalObject(Math.abs((val - _this.pg_rank))));
                _this.pg_rank_nxt = val ;
                _this.pg_rank = _this.pg_rank_nxt ;
            }
        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class pagerankVertexFactory
        extends NullEdgeVertexFactory< pagerank.VertexData, pagerank.MessageData > {
        @Override
        public NullEdgeVertex< pagerank.VertexData, pagerank.MessageData > newInstance(CommandLine line) {
            return new pagerankVertex(line);
        }
    } // end of VertexFactory

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // properties
        double pg_rank;
        double pg_rank_nxt;
        double _S1prop;

        @Override
        public int numBytes() {return 24;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putDouble(pg_rank);
            IOB.putDouble(pg_rank_nxt);
            IOB.putDouble(_S1prop);
        }
        @Override
        public void read(IoBuffer IOB) {
            pg_rank= IOB.getDouble();
            pg_rank_nxt= IOB.getDouble();
            _S1prop= IOB.getDouble();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            pg_rank= Utils.byteArrayToDoubleBigEndian(_BA, _idx + 0);
            pg_rank_nxt= Utils.byteArrayToDoubleBigEndian(_BA, _idx + 8);
            _S1prop= Utils.byteArrayToDoubleBigEndian(_BA, _idx + 16);
            return 24;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 24);
            return 24;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "" + "pg_rank: " + pg_rank;
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
        double d0;

        @Override
        public int numBytes() {
            return 8; // data
        }
        @Override
        public void write(IoBuffer IOB) {
            IOB.putDouble(d0);
        }
        @Override
        public void read(IoBuffer IOB) {
            d0= IOB.getDouble();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            d0= Utils.byteArrayToDoubleBigEndian(_BA, _idx + 0);
            return 8;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx+0, 8);
            return 8;
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
            return pagerankMaster.class;
        }
        @Override
        public Class<?> getVertexClass() {
            return pagerankVertex.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return pagerankVertexFactory.class;
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
