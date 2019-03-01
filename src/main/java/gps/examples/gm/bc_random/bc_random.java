package gps.examples.gm.bc_random;
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

public class bc_random{

    // Keys for shared_variables 
    private static final String KEY_s = "s";
    private static final String KEY_curr_level = "curr_level";
    private static final String KEY_bfs_finished = "bfs_finished";

    public static class bc_randomMaster extends Master {
        // Control fields
        private int     _master_state                = 100000;
        private int     _master_state_nxt            = 100000;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public bc_randomMaster (CommandLine line) {
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
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private int K;
        private int k;
        private int s;
        private int curr_level;
        private boolean bfs_finished;

        //----------------------------------------------------------
        // Master's State-machine 
        //----------------------------------------------------------
        private void _master_state_machine() {
            _master_should_start_workers = false;
            _master_should_finish = false;
            do {
                _master_state = _master_state_nxt ;
                switch(_master_state) {
                    case 100000: _master_state_100000(); break;
                    case 100001: _master_state_100001(); break;
                    case 0: _master_state_0(); break;
                    case 2: _master_state_2(); break;
                    case 3: _master_state_3(); break;
                    case 4: _master_state_4(); break;
                    case 5: _master_state_5(); break;
                    case 9: _master_state_9(); break;
                    case 10: _master_state_10(); break;
                    case 13: _master_state_13(); break;
                    case 14: _master_state_14(); break;
                    case 18: _master_state_18(); break;
                    case 33: _master_state_33(); break;
                    case 34: _master_state_34(); break;
                    case 19: _master_state_19(); break;
                    case 36: _master_state_36(); break;
                    case 21: _master_state_21(); break;
                    case 16: _master_state_16(); break;
                    case 22: _master_state_22(); break;
                    case 23: _master_state_23(); break;
                    case 27: _master_state_27(); break;
                    case 28: _master_state_28(); break;
                    case 38: _master_state_38(); break;
                    case 30: _master_state_30(); break;
                    case 25: _master_state_25(); break;
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

        private void _master_state_100000() {
            /*------
            -----*/
            System.out.println("Running _master_state 100000");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            // Preparation Step;
            _master_state_nxt = 100001;
            _master_should_start_workers = true;
        }
        private void _master_state_100001() {
            /*------
            -----*/
            System.out.println("Running _master_state 100001");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            // Preparation Step;
            _master_state_nxt = 0;
            _master_should_start_workers = true;
        }
        private void _master_state_0() {
            /*------
            k = 0;
            -----*/
            System.out.println("Running _master_state 0");

            k = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (t1 : G.Nodes)
                t1.BC =  (Float ) 0;
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

            _master_state_nxt = 4;
        }
        private void _master_state_4() {
        	System.out.println("Incrementing k.");
        	k++;
        	/*------
            k < K
            -----*/
            System.out.println("Running _master_state 4");
            // While (...)
            boolean _expression_result = k < K;
            if (_expression_result) _master_state_nxt = 5;
            else _master_state_nxt = 7;

            if (_expression_result) _master_state_nxt = 5;
            else _master_state_nxt = 7;

        }
        private void _master_state_5() {
            /*------
            s = G.PickRandom();
            -----*/
            System.out.println("Running _master_state 5");
            
            s = (new java.util.Random()).nextInt(getGraphSize());
            _master_state_nxt = 9;
        }
        private void _master_state_9() {
            /*------
            Foreach (t2 : G.Nodes)
            {
                t2.sigma =  (Float ) 0;
                If (t2 == s)
                    t2.sigma =  (Float ) 1;
            }

            Foreach (i : G.Nodes)
            {
                i.level = (i == s)  ? 0 : +INF;
            }
            -----*/
            System.out.println("Running _master_state 9");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_s,new IntOverwriteGlobalObject(s));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 10;
            _master_should_start_workers = true;
        }
        private void _master_state_10() {
            /*------
            curr_level = -1;
            bfs_finished = False;
            -----*/
            System.out.println("Running _master_state 10");

            curr_level = -1 ;
            bfs_finished = false ;
            _master_state_nxt = 13;
        }
        private void _master_state_13() {
            /*------
            bfs_finished != True
            -----*/
            System.out.println("Running _master_state 13");
            // While (...)

            boolean _expression_result = bfs_finished != true;
            if (_expression_result) _master_state_nxt = 14;
            else _master_state_nxt = 16;

            if (_expression_result) _master_state_nxt = 14;
            else _master_state_nxt = 16;

        }
        private void _master_state_14() {
            /*------
            bfs_finished = True;
            curr_level = curr_level + 1;
            -----*/
            System.out.println("Running _master_state 14");

            bfs_finished = true ;
            curr_level = curr_level + 1 ;
            _master_state_nxt = 18;
        }
        private void _master_state_18() {
            /*------
            Foreach (v : G.Nodes)
            {
                If (v.level == curr_level)
                {
                    Foreach (_t : v.Nbrs)
                    {
                        If (_t.level == +INF)
                        {
                            _t.level = curr_level + 1;
                            bfs_finished &= False @ v ;
                        }
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 18");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_curr_level,new IntOverwriteGlobalObject(curr_level));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_bfs_finished,new BooleanANDGlobalObject(true));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 33;
            _master_should_start_workers = true;
        }
        private void _master_state_33() {
            /*------
            -----*/
            System.out.println("Running _master_state 33");
            bfs_finished = bfs_finished&&((BooleanANDGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY_bfs_finished)).getValue().getValue();

            _master_state_nxt = 34;
        }
        private void _master_state_34() {
            /*------
            //Receive Nested Loop
            Foreach (_t : v.Nbrs)
            {
                If (_t.level == +INF)
                {
                    _t.level = curr_level + 1;
                    bfs_finished &= False @ v ;
                }
            }
            Foreach (v : G.Nodes)
            {
                If (v.level == curr_level)
                {
                    Foreach (w : v.Nbrs)
                    {
                        _m6 = v.sigma;
                        If (w.level == curr_level + 1)
                        {
                            w.sigma += _m6 @ v ;
                        }
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 34");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_curr_level,new IntOverwriteGlobalObject(curr_level));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_bfs_finished,new BooleanANDGlobalObject(true));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 19;
            _master_should_start_workers = true;
        }
        private void _master_state_19() {
            /*------
            -----*/
            System.out.println("Running _master_state 19");
            bfs_finished = bfs_finished&&((BooleanANDGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY_bfs_finished)).getValue().getValue();

            _master_state_nxt = 36;
        }
        private void _master_state_36() {
            /*------
            //Receive Nested Loop
            Foreach (w : v.Nbrs)
            {
                _m6 = v.sigma;
                If (w.level == curr_level + 1)
                {
                    w.sigma += _m6 @ v ;
                }
            }
            -----*/
            System.out.println("Running _master_state 36");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_curr_level,new IntOverwriteGlobalObject(curr_level));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 21;
            _master_should_start_workers = true;
        }
        private void _master_state_21() {
            /*------
            -----*/
            System.out.println("Running _master_state 21");

            _master_state_nxt = 13;
        }
        private void _master_state_16() {
            /*------
            -----*/
            System.out.println("Running _master_state 16");

            _master_state_nxt = 22;
        }
        private void _master_state_22() {
            /*------
            curr_level >= 0
            -----*/
            System.out.println("Running _master_state 22");
            // While (...)

            boolean _expression_result = curr_level >= 0;
            if (_expression_result) _master_state_nxt = 23;
            else _master_state_nxt = 25;

            if (_expression_result) _master_state_nxt = 23;
            else _master_state_nxt = 25;

        }
        private void _master_state_23() {
            /*------
            -----*/
            System.out.println("Running _master_state 23");

            _master_state_nxt = 27;
        }
        private void _master_state_27() {
            /*------
            Foreach (v : G.Nodes)
            {
                If (v.level == curr_level)
                {
                    v._S4prop = 0.000000;
                }
            }

            Foreach (w3 : G.Nodes)
            {
                If (w3.level == curr_level + 1)
                {
                    Foreach (v : w3.InNbrs)
                    {
                        _m7 = w3.sigma;
                        _m8 = (1 + w3.delta) ;
                        If (v.level == curr_level)
                            v._S4prop += (v.sigma / _m7 * _m8)  @ w3 ;
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 27");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_curr_level,new IntOverwriteGlobalObject(curr_level));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 28;
            _master_should_start_workers = true;
        }
        private void _master_state_28() {
            /*------
            -----*/
            System.out.println("Running _master_state 28");

            _master_state_nxt = 38;
        }
        private void _master_state_38() {
            /*------
            //Receive Nested Loop
            Foreach (v : w3.InNbrs)
            {
                _m7 = w3.sigma;
                _m8 = (1 + w3.delta) ;
                If (v.level == curr_level)
                    v._S4prop += (v.sigma / _m7 * _m8)  @ w3 ;
            }
            Foreach (v : G.Nodes)
            {
                If (v.level == curr_level)
                {
                    v.delta = v._S4prop;
                    v.BC = v.BC + v.delta;
                }
            }
            -----*/
            System.out.println("Running _master_state 38");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_curr_level,new IntOverwriteGlobalObject(curr_level));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 30;
            _master_should_start_workers = true;
        }
        private void _master_state_30() {
            /*------
            curr_level = curr_level - 1;
            -----*/
            System.out.println("Running _master_state 30");

            curr_level = curr_level - 1 ;
            _master_state_nxt = 22;
        }
        private void _master_state_25() {
            /*------
            -----*/
            System.out.println("Running _master_state 25");

            _master_state_nxt = 4;
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
    public static class bc_randomVertex
        extends NullEdgeVertex< bc_random.VertexData, bc_random.MessageData > {

        public bc_randomVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }

        @Override
        public void compute(Iterable<bc_random.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 100000: _vertex_state_100000(_msgs); break;
                case 100001: _vertex_state_100001(_msgs); break;
                case 2: _vertex_state_2(_msgs); break;
                case 9: _vertex_state_9(_msgs); break;
                case 18: _vertex_state_18(_msgs); break;
                case 34: _vertex_state_34(_msgs); break;
                case 36: _vertex_state_36(_msgs); break;
                case 27: _vertex_state_27(_msgs); break;
                case 38: _vertex_state_38(_msgs); break;
            }
        }
        private void _vertex_state_100000(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            // Preperation: creating reverse edges
            int _remoteNodeId = getId();
            // Sending messages to all neighbors (if there is a neighbor)
            if (getNeighborsSize() > 0) {
                MessageData _msg = new MessageData((byte) 0);
                _msg.i0 = _remoteNodeId;
                sendMessages(getNeighborIds(), _msg);
            }
        }
        private void _vertex_state_100001(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            //Preperation creating reverse edges
            int i = 0; // iterable does not have length(), so we have to count it
            for(MessageData _msg : _msgs) i++;
            _this._revNodeId = new int[i];

            i=0;
            for(MessageData _msg : _msgs) {
                int _remoteNodeId = _msg.i0;
                _this._revNodeId[i] = _remoteNodeId;
                i++;
            }
        }
        private void _vertex_state_2(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t1 : G.Nodes)
                t1.BC =  (Float ) 0;
            -----*/

            _this.BC = (float)0 ;
        }
        private void _vertex_state_9(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            int s = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_s)).getValue().getValue();
            /*------
            Foreach (t2 : G.Nodes)
            {
                t2.sigma =  (Float ) 0;
                If (t2 == s)
                    t2.sigma =  (Float ) 1;
            }

            Foreach (i : G.Nodes)
            {
                i.level = (i == s)  ? 0 : +INF;
            }
            -----*/

            {
                _this.sigma = (float)0 ;
                if (getId() == s)
                    _this.sigma = (float)1 ;
            }

            {
                _this.level = (getId() == s)?0:Integer.MAX_VALUE ;
            }
        }
        private void _vertex_state_18(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            int curr_level = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_curr_level)).getValue().getValue();
            /*------
            Foreach (v : G.Nodes)
            {
                If (v.level == curr_level)
                {
                    Foreach (_t : v.Nbrs)
                    {
                        If (_t.level == +INF)
                        {
                            _t.level = curr_level + 1;
                            bfs_finished &= False @ v ;
                        }
                    }
                }
            }
            -----*/

            {
                if (_this.level == curr_level)
                {
                    // Sending messages to all neighbors (if there is a neighbor)
                    if (getNeighborsSize() > 0) {
                        MessageData _msg = new MessageData((byte) 1);
                        sendMessages(getNeighborIds(), _msg);
                    }
                }
            }
        }
        private void _vertex_state_34(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            int curr_level = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_curr_level)).getValue().getValue();
            float _m6;

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                Foreach (_t : v.Nbrs)
                {
                    If (_t.level == +INF)
                    {
                        _t.level = curr_level + 1;
                        bfs_finished &= False @ v ;
                    }
                }
                -----*/
                if (_this.level == Integer.MAX_VALUE)
                {
                    _this.level = curr_level + 1 ;
                    getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_bfs_finished,new BooleanANDGlobalObject(false));
                }
            }

            /*------
            Foreach (v : G.Nodes)
            {
                If (v.level == curr_level)
                {
                    Foreach (w : v.Nbrs)
                    {
                        _m6 = v.sigma;
                        If (w.level == curr_level + 1)
                        {
                            w.sigma += _m6 @ v ;
                        }
                    }
                }
            }
            -----*/

            {
                if (_this.level == curr_level)
                {
                    // Sending messages to all neighbors (if there is a neighbor)
                    if (getNeighborsSize() > 0) {
                        MessageData _msg = new MessageData((byte) 2);
                        _m6 = _this.sigma ;
                        _msg.f0 = _m6;
                        sendMessages(getNeighborIds(), _msg);
                    }
                }
            }
        }
        private void _vertex_state_36(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            int curr_level = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_curr_level)).getValue().getValue();
            float _m6;

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                Foreach (w : v.Nbrs)
                {
                    _m6 = v.sigma;
                    If (w.level == curr_level + 1)
                    {
                        w.sigma += _m6 @ v ;
                    }
                }
                -----*/
                _m6 = _msg.f0;
                if (_this.level == curr_level + 1)
                {
                    _this.sigma = _this.sigma + (_m6);
                }
            }

        }
        private void _vertex_state_27(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            int curr_level = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_curr_level)).getValue().getValue();
            float _m8;
            float _m7;
            /*------
            Foreach (v : G.Nodes)
            {
                If (v.level == curr_level)
                {
                    v._S4prop = 0.000000;
                }
            }

            Foreach (w3 : G.Nodes)
            {
                If (w3.level == curr_level + 1)
                {
                    Foreach (v : w3.InNbrs)
                    {
                        _m7 = w3.sigma;
                        _m8 = (1 + w3.delta) ;
                        If (v.level == curr_level)
                            v._S4prop += (v.sigma / _m7 * _m8)  @ w3 ;
                    }
                }
            }
            -----*/

            {
                if (_this.level == curr_level)
                {
                    _this._S4prop = (float)(0.000000) ;
                }
            }

            {
                if (_this.level == curr_level + 1)
                {
                    // Sending messages to all neighbors (if there is a neighbor)
                    if (_this._revNodeId.length > 0) {
                        MessageData _msg = new MessageData((byte) 3);
                        _m7 = _this.sigma ;
                        _m8 = (1 + _this.delta) ;
                        _msg.f0 = _m7;
                        _msg.f1 = _m8;
                        sendMessages(_this._revNodeId, _msg);
                    }
                }
            }
        }
        private void _vertex_state_38(Iterable<bc_random.MessageData> _msgs) {
            VertexData _this = getValue();
            int curr_level = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_curr_level)).getValue().getValue();
            float _m8;
            float _m7;

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                Foreach (v : w3.InNbrs)
                {
                    _m7 = w3.sigma;
                    _m8 = (1 + w3.delta) ;
                    If (v.level == curr_level)
                        v._S4prop += (v.sigma / _m7 * _m8)  @ w3 ;
                }
                -----*/
                _m7 = _msg.f0;
                _m8 = _msg.f1;
                if (_this.level == curr_level)
                    _this._S4prop = _this._S4prop + ((_this.sigma / _m7 * _m8));
            }

            /*------
            Foreach (v : G.Nodes)
            {
                If (v.level == curr_level)
                {
                    v.delta = v._S4prop;
                    v.BC = v.BC + v.delta;
                }
            }
            -----*/

            {
                if (_this.level == curr_level)
                {
                    _this.delta = _this._S4prop ;
                    _this.BC = _this.BC + _this.delta ;
                }
            }
        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class bc_randomVertexFactory
        extends NullEdgeVertexFactory< bc_random.VertexData, bc_random.MessageData > {
        @Override
        public NullEdgeVertex< bc_random.VertexData, bc_random.MessageData > newInstance(CommandLine line) {
            return new bc_randomVertex(line);
        }
    } // end of VertexFactory

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // properties
        float BC;
        float sigma;
        float delta;
        int level;
        float _S4prop;
        int [] _revNodeId; //reverse edges (node IDs) {should this to be marshalled?}

        @Override
        public int numBytes() {return 20;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putFloat(BC);
            IOB.putFloat(sigma);
            IOB.putFloat(delta);
            IOB.putInt(level);
            IOB.putFloat(_S4prop);
        }
        @Override
        public void read(IoBuffer IOB) {
            BC= IOB.getFloat();
            sigma= IOB.getFloat();
            delta= IOB.getFloat();
            level= IOB.getInt();
            _S4prop= IOB.getFloat();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            BC= Utils.byteArrayToFloatBigEndian(_BA, _idx + 0);
            sigma= Utils.byteArrayToFloatBigEndian(_BA, _idx + 4);
            delta= Utils.byteArrayToFloatBigEndian(_BA, _idx + 8);
            level= Utils.byteArrayToIntBigEndian(_BA, _idx + 12);
            _S4prop= Utils.byteArrayToFloatBigEndian(_BA, _idx + 16);
            return 20;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 20);
            return 20;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "" + "BC: " + BC;
        }
    } // end of data class

    //----------------------------------------------
    // Message Data 
    //----------------------------------------------
    public static class MessageData extends MinaWritable {
        byte m_type;
        public MessageData(byte type) {m_type = type;}


        public MessageData() {
            // default constructor that is required for constructing a representative instance for IncomingMessageStore.
        }
        // union of all message fields  
        int i0;
        float f0;
        float f1;

        @Override
        public int numBytes() {
            if (m_type == 0) return (1+4); // type + data
            else if (m_type == 1) return (1+0); // type + data
            else if (m_type == 2) return (1+4); // type + data
            else if (m_type == 3) return (1+8); // type + data
            //for empty messages (signaling only)
            return 1; 
        }
        @Override
        public void write(IoBuffer IOB) {
//        	System.out.println("Writing a message: type: " + m_type);
            IOB.put(m_type);
            if (m_type == 0) {
//            	System.out.println("Writing int: " + i0);
                IOB.putInt(i0);
            }
            else if (m_type == 1) {
            }
            else if (m_type == 2) {
                IOB.putFloat(f0);
            }
            else if (m_type == 3) {
                IOB.putFloat(f0);
                IOB.putFloat(f1);
            }
            //for empty messages (signaling only)
        }
        @Override
        public void read(IoBuffer IOB) {
            m_type = IOB.get();
            if (m_type == 0) {
                i0= IOB.getInt();
            }
            else if (m_type == 1) {
            }
            else if (m_type == 2) {
                f0= IOB.getFloat();
            }
            else if (m_type == 3) {
                f0= IOB.getFloat();
                f1= IOB.getFloat();
            }
            //for empty messages (signaling only)
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            m_type = _BA[_idx];
            if (m_type == 0) {
                i0= Utils.byteArrayToIntBigEndian(_BA, _idx + 1);
                return 1 + 4;
            }
            else if (m_type == 1) {
                return 1 + 0;
            }
            else if (m_type == 2) {
                f0= Utils.byteArrayToFloatBigEndian(_BA, _idx + 1);
                return 1 + 4;
            }
            else if (m_type == 3) {
                f0= Utils.byteArrayToFloatBigEndian(_BA, _idx + 1);
                f1= Utils.byteArrayToFloatBigEndian(_BA, _idx + 5);
                return 1 + 8;
            }
            //for empty messages (signaling only)
            return 1;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 1);
            byte m_type = _BA[_idx];
//            byte m_type = IOB.get(_BA, _idx, 1).get();
            if (m_type == 0) {
                IOB.get(_BA, _idx+1, 4);
                return 1 + 4;
            }
            else if (m_type == 1) {
                IOB.get(_BA, _idx+1, 0);
                return 1 + 0;
            }
            else if (m_type == 2) {
                IOB.get(_BA, _idx+1, 4);
                return 1 + 4;
            }
            else if (m_type == 3) {
                IOB.get(_BA, _idx+1, 8);
                return 1 + 8;
            }
            //for empty messages (signaling only)
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
            return bc_randomMaster.class;
        }
        @Override
        public Class<?> getVertexClass() {
            return bc_randomVertex.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return bc_randomVertexFactory.class;
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
