package gps.examples.gm.sssp_with_edge_values;
import gps.*;
import gps.graph.*;
import gps.node.*;
import gps.writable.*;
import gps.globalobjects.*;
import org.apache.commons.cli.CommandLine;
import org.apache.mina.core.buffer.IoBuffer;
import java.io.IOException;
import java.io.BufferedWriter;
import java.util.Random;
import java.lang.Math;

public class sssp{

    // Keys for shared_variables 
    private static final String KEY_root = "root";
    private static final String KEY__E8 = "_E8";

    public static class ssspMaster extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public ssspMaster (CommandLine line) {
            // parse command-line arguments (if any)
            java.util.HashMap<String,String> arg_map = new java.util.HashMap<String,String>();
            gps.node.Utils.parseOtherOptions(line, arg_map);

            if (arg_map.containsKey("root")) {
                String s = arg_map.get("root");
                root = Integer.parseInt(s);
            }
        }

        //save output
        public void writeOutput(BufferedWriter bw) throws IOException {
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private int root;
        private boolean fin;
        private boolean _E8;
        private boolean _is_first_4 = true;

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
                    case 4: _master_state_4(); break;
                    case 5: _master_state_5(); break;
                    case 14: _master_state_14(); break;
                    case 10: _master_state_10(); break;
                    case 15: _master_state_15(); break;
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
            fin = False;
            -----*/
            System.out.println("Running _master_state 0");

            fin = false ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0.dist = (t0 == root)  ? 0 : +INF;
                t0.updated = (t0 == root)  ? True : False;
                t0.dist_nxt = t0.dist;
                t0.updated_nxt = t0.updated;
            }
            -----*/
            System.out.println("Running _master_state 2");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_root,new IntOverwriteGlobalObject(root));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 3;
            _master_should_start_workers = true;
        }
        private void _master_state_3() {
            /*------
            -----*/
            System.out.println("Running _master_state 3");

            _master_state_nxt = 4;
        }
        private void _master_state_4() {
            /*------
            !fin
            -----*/
            System.out.println("Running _master_state 4");
            // While (...)

            boolean _expression_result =  !fin;
            if (_expression_result) _master_state_nxt = 5;
            else _master_state_nxt = 7;

            if (!_expression_result) _is_first_4=true; // reset is_first


        }
        private void _master_state_5() {
            /*------
            fin = True;
            _E8 = False;
            -----*/
            System.out.println("Running _master_state 5");

            fin = true ;
            _E8 = false ;
            _master_state_nxt = 14;
        }
        private void _master_state_14() {
            /*------
            //Receive Nested Loop
            Foreach (s : n.Nbrs)
            {
                e = s.ToEdge();
                _m9 = n.dist + e.len;
                <s.dist_nxt ; s.updated_nxt> min= <_m9 ; True> @ n ;
            }
            Foreach (t4 : G.Nodes)
            {
                t4.dist = t4.dist_nxt;
                t4.updated = t4.updated_nxt;
                t4.updated_nxt = False;
                _E8 |= t4.updated @ t4 ;
            }

            Foreach (n : G.Nodes)
            {
                If (n.updated)
                {
                    Foreach (s : n.Nbrs)
                    {
                        e = s.ToEdge();
                        _m9 = n.dist + e.len;
                        <s.dist_nxt ; s.updated_nxt> min= <_m9 ; True> @ n ;
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 14");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__E8,new BooleanOrGlobalObject(false));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            getGlobalObjectsMap().putOrUpdateGlobalObject("_is_first_4",new BooleanOverwriteGlobalObject(_is_first_4));

            _master_state_nxt = 10;
            _master_should_start_workers = true;
        }
        private void _master_state_10() {
            /*------
            fin = !_E8;
            -----*/
            System.out.println("Running _master_state 10");
            _E8 = _E8||((BooleanOrGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY__E8)).getValue().getValue();

            if (!_is_first_4) {
                fin =  !_E8 ;
            }
            _master_state_nxt = 15;
        }
        private void _master_state_15() {
            /*------
            -----*/
            System.out.println("Running _master_state 15");
            // Intra-Loop Merged (tail)
            if (_is_first_4) _master_state_nxt = 5;
            else _master_state_nxt = 4;
            _is_first_4 = false;

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
    public static class ssspVertex
        extends Vertex< sssp.VertexData, sssp.EdgeData, sssp.MessageData > {

        public ssspVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }

        @Override
        public void compute(Iterable<sssp.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 14: _vertex_state_14(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<sssp.MessageData> _msgs) {
            VertexData _this = getValue();
            int root = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_root)).getValue().getValue();
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0.dist = (t0 == root)  ? 0 : +INF;
                t0.updated = (t0 == root)  ? True : False;
                t0.dist_nxt = t0.dist;
                t0.updated_nxt = t0.updated;
            }
            -----*/

            {
                _this.dist = (getId() == root)?0:Integer.MAX_VALUE ;
                _this.updated = (getId() == root)?true:false ;
                _this.dist_nxt = _this.dist ;
                _this.updated_nxt = _this.updated ;
            }
        }
        private void _vertex_state_14(Iterable<sssp.MessageData> _msgs) {
            VertexData _this = getValue();
            int _m9;
            boolean _is_first_4 = ((BooleanOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("_is_first_4")).getValue().getValue();

            if (!_is_first_4) {
                // Begin msg receive
                for(MessageData _msg : _msgs) {
                    /*------
                    (Nested Loop)
                    Foreach (s : n.Nbrs)
                    {
                        e = s.ToEdge();
                        _m9 = n.dist + e.len;
                        <s.dist_nxt ; s.updated_nxt> min= <_m9 ; True> @ n ;
                    }
                    -----*/
                    _m9 = _msg.i0;
                    if (_this.dist_nxt > _m9) {
                        _this.dist_nxt = _m9;
                        _this.updated_nxt = true;
                    }
                }
            }

            /*------
            Foreach (t4 : G.Nodes)
            {
                t4.dist = t4.dist_nxt;
                t4.updated = t4.updated_nxt;
                t4.updated_nxt = False;
                _E8 |= t4.updated @ t4 ;
            }

            Foreach (n : G.Nodes)
            {
                If (n.updated)
                {
                    Foreach (s : n.Nbrs)
                    {
                        e = s.ToEdge();
                        _m9 = n.dist + e.len;
                        <s.dist_nxt ; s.updated_nxt> min= <_m9 ; True> @ n ;
                    }
                }
            }
            -----*/

            if (!_is_first_4)
            {
                _this.dist = _this.dist_nxt ;
                _this.updated = _this.updated_nxt ;
                _this.updated_nxt = false ;
                getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__E8,new BooleanOrGlobalObject(_this.updated));
            }

            {
                if (_this.updated)
                {
                    for (Edge<EdgeData> _outEdge : getOutgoingEdges()) {
                        // Sending messages to each neighbor
                        MessageData _msg = new MessageData((byte) 0);
                        _m9 = _this.dist + _outEdge.getEdgeValue().len ;
                        _msg.i0 = _m9;
                        sendMessage(_outEdge.getNeighborId(), _msg);
                    }
                }
            }
        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class ssspVertexFactory
        extends VertexFactory< sssp.VertexData, sssp.EdgeData, sssp.MessageData > {
        @Override
        public Vertex< sssp.VertexData, sssp.EdgeData, sssp.MessageData > newInstance(CommandLine line) {
            return new ssspVertex(line);
        }
    } // end of VertexFactory

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // properties
        int dist;
        boolean updated;
        boolean updated_nxt;
        int dist_nxt;

        @Override
        public int numBytes() {return 10;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(dist);
            IOB.put(updated?(byte)1:(byte)0);
            IOB.put(updated_nxt?(byte)1:(byte)0);
            IOB.putInt(dist_nxt);
        }
        @Override
        public void read(IoBuffer IOB) {
            dist= IOB.getInt();
            updated= IOB.get()==0?false:true;
            updated_nxt= IOB.get()==0?false:true;
            dist_nxt= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            dist= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            updated= Utils.byteArrayToBooleanBigEndian(_BA, _idx + 4);
            updated_nxt= Utils.byteArrayToBooleanBigEndian(_BA, _idx + 5);
            dist_nxt= Utils.byteArrayToIntBigEndian(_BA, _idx + 6);
            return 10;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 10);
            return 10;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "" + "dist: " + dist;
        }
    } // end of data class

    //----------------------------------------------
    // Edge Property Class
    //----------------------------------------------
    public static class EdgeData extends MinaWritable {
        // properties
        int len;

        @Override
        public int numBytes() {return 4;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(len);
        }
        @Override
        public void read(IoBuffer IOB) {
            len= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            len= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            return 4;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 4);
            return 4;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "";
        }
        //Input Data Parsing
        @Override
        public void read(String inputString) {
            this.len = Integer.parseInt(inputString);
        }
    } // end of data class

    //----------------------------------------------
    // Message Data 
    //----------------------------------------------
    public static class MessageData extends MinaWritable {
        //single message type; argument ignored
        public MessageData(byte type) {}


        public MessageData() {
            // default constructor that is required for constructing a representative instance for IncomingMessageStore.
        }
        // union of all message fields  
        int i0;

        @Override
        public int numBytes() {
            return 4; // data
        }
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(i0);
        }
        @Override
        public void read(IoBuffer IOB) {
            i0= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            i0= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            return 4;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx+0, 4);
            return 4;
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
            return ssspMaster.class;
        }
        @Override
        public Class<?> getVertexClass() {
            return ssspVertex.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return ssspVertexFactory.class;
        }
        @Override
        public Class<?> getVertexValueClass() {
            return VertexData.class;
        }
        @Override
        public Class<?> getEdgeValueClass() {
            return EdgeData.class;
        }
        @Override
        public Class<?> getMessageValueClass() {
            return MessageData.class;
        }
    }
}
