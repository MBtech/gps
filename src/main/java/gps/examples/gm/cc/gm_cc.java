package gps.examples.gm.cc;
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

public class gm_cc{

    // Keys for shared_variables 
    private static final String KEY__E8 = "_E8";

    public static class gm_connectedcomponentsMaster extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public gm_connectedcomponentsMaster (CommandLine line) {
            // parse command-line arguments (if any)
            java.util.HashMap<String,String> arg_map = new java.util.HashMap<String,String>();
            gps.node.Utils.parseOtherOptions(line, arg_map);

        }

        //save output
        public void writeOutput(BufferedWriter bw) throws IOException {
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private boolean fin;
        private boolean _E8;

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
                    case 9: _master_state_9(); break;
                    case 13: _master_state_13(); break;
                    case 14: _master_state_14(); break;
                    case 10: _master_state_10(); break;
                    case 11: _master_state_11(); break;
                    case 12: _master_state_12(); break;
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
                t0.cid = 1;
                t0.updated = True;
                t0.cid_nxt = t0.cid;
                t0.updated_nxt = False;
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

        }
        private void _master_state_5() {
            /*------
            fin = True;
            _E8 = False;
            -----*/
            System.out.println("Running _master_state 5");
            fin = true ;
            _E8 = false ;
            _master_state_nxt = 9;
        }
        private void _master_state_9() {
            /*------
            Foreach (n : G.Nodes)
            {
                If (n.updated)
                {
                    Foreach (s : n.Nbrs)
                    {
                        <s.cid_nxt ; s.updated_nxt> min= <n.cid ; True> @ n ;
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 9");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 13;
            _master_should_start_workers = true;
        }
        private void _master_state_13() {
            /*------
            -----*/
            System.out.println("Running _master_state 13");
            _master_state_nxt = 14;
        }
        private void _master_state_14() {
            /*------
            //Receive Nested Loop
            Foreach (s : n.Nbrs)
            {
                <s.cid_nxt ; s.updated_nxt> min= <n.cid ; True> @ n ;
            }
            -----*/
            System.out.println("Running _master_state 14");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 10;
            _master_should_start_workers = true;
        }
        private void _master_state_10() {
            /*------
            -----*/
            System.out.println("Running _master_state 10");
            _master_state_nxt = 11;
        }
        private void _master_state_11() {
            /*------
            Foreach (t4 : G.Nodes)
            {
                t4.cid = t4.cid_nxt;
                t4.updated = t4.updated_nxt;
                t4.updated_nxt = False;
                _E8 |= t4.updated @ t4 ;
            }
            -----*/
            System.out.println("Running _master_state 11");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 12;
            _master_should_start_workers = true;
        }
        private void _master_state_12() {
            /*------
            fin = !_E8;
            -----*/
            System.out.println("Running _master_state 12");
            _E8 = ((BooleanOrGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY__E8)).getValue().getValue();
            fin =  !_E8 ;
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
    public static class gm_connectedcomponentsVertex
        extends NullEdgeVertex< gm_cc.VertexData, gm_cc.MessageData > {

        public gm_connectedcomponentsVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }

        @Override
        public void compute(Iterable<gm_cc.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 9: _vertex_state_9(_msgs); break;
                case 14: _vertex_state_14(_msgs); break;
                case 11: _vertex_state_11(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<gm_cc.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0.cid = 1;
                t0.updated = True;
                t0.cid_nxt = t0.cid;
                t0.updated_nxt = False;
            }
            -----*/

            {
                _this.cid = getId() ;
                _this.updated = true ;
                _this.cid_nxt = _this.cid ;
                _this.updated_nxt = false ;
            }
        }
        private void _vertex_state_9(Iterable<gm_cc.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (n : G.Nodes)
            {
                If (n.updated)
                {
                    Foreach (s : n.Nbrs)
                    {
                        <s.cid_nxt ; s.updated_nxt> min= <n.cid ; True> @ n ;
                    }
                }
            }
            -----*/

            {
                if (_this.updated)
                {
                    // Sending messages to all neighbors
                    MessageData _msg = new MessageData((byte) 0);
                    _msg.i0 = _this.cid;
                    sendMessages(getNeighborIds(), _msg);
                }
            }
        }
        private void _vertex_state_14(Iterable<gm_cc.MessageData> _msgs) {
            VertexData _this = getValue();

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                Foreach (s : n.Nbrs)
                {
                    <s.cid_nxt ; s.updated_nxt> min= <n.cid ; True> @ n ;
                }
                -----*/
                int _remote_cid = _msg.i0;
                if (_this.cid_nxt > _remote_cid) {
                    _this.cid_nxt = _remote_cid;
                    _this.updated_nxt = true;
                }
            }

        }
        private void _vertex_state_11(Iterable<gm_cc.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t4 : G.Nodes)
            {
                t4.cid = t4.cid_nxt;
                t4.updated = t4.updated_nxt;
                t4.updated_nxt = False;
                _E8 |= t4.updated @ t4 ;
            }
            -----*/

            {
                _this.cid = _this.cid_nxt ;
                _this.updated = _this.updated_nxt ;
                _this.updated_nxt = false ;
                getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__E8,new BooleanOrGlobalObject(_this.updated));
            }
        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class gm_connectedcomponentsVertexFactory
        extends NullEdgeVertexFactory< gm_cc.VertexData, gm_cc.MessageData > {
        @Override
        public NullEdgeVertex< gm_cc.VertexData, gm_cc.MessageData > newInstance(CommandLine line) {
            return new gm_connectedcomponentsVertex(line);
        }
    } // end of VertexFactory

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // properties
        int cid;
        boolean updated;
        boolean updated_nxt;
        int cid_nxt;

        @Override
        public int numBytes() {return 10;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(cid);
            IOB.put(updated?(byte)1:(byte)0);
            IOB.put(updated_nxt?(byte)1:(byte)0);
            IOB.putInt(cid_nxt);
        }
        @Override
        public void read(IoBuffer IOB) {
            cid= IOB.getInt();
            updated= IOB.get()==0?false:true;
            updated_nxt= IOB.get()==0?false:true;
            cid_nxt= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            cid= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            updated= Utils.byteArrayToBooleanBigEndian(_BA, _idx + 4);
            updated_nxt= Utils.byteArrayToBooleanBigEndian(_BA, _idx + 5);
            cid_nxt= Utils.byteArrayToIntBigEndian(_BA, _idx + 6);
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
            return "" + "cid: " + cid + "\tupdated: " + updated + "\tupdated_nxt: " + updated_nxt + "\tcid_nxt: " + cid_nxt;
        }
    } // end of vertex-data

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
            return gm_connectedcomponentsMaster.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return gm_connectedcomponentsVertexFactory.class;
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
		@Override
		public Class<?> getVertexClass() {
			return gm_connectedcomponentsVertex.class;
		}
    }
}
