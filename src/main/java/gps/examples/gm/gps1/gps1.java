package gps.examples.gm.gps1;
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

public class gps1{
    
    // Keys for shared_variables 
    private static final String KEY__S0 = "_S0";
    
    public static class gps1Master extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;
        
        public gps1Master (CommandLine line) {
            // parse command-line arguments (if any)
            java.util.HashMap<String,String> arg_map = new java.util.HashMap<String,String>();
            gps.node.Utils.parseOtherOptions(line, arg_map);
            
            if (arg_map.containsKey("x")) {
                String s = arg_map.get("x");
                x = Integer.parseInt(s);
            }
        }
        
        //save output
        public void writeOutput(BufferedWriter bw) throws IOException {
            bw.write("_ret_value:\t" + _ret_value + "\n");
        }
        
        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private int _S0;
        private int x;
        private int z;
        private int _ret_value; // the final return value of the procedure
        
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
                    case 4: _master_state_4(); break;
                    case 6: _master_state_6(); break;
                    case 7: _master_state_7(); break;
                    case 3: _master_state_3(); break;
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
            z = 0;
            -----*/
            System.out.println("Running _master_state 0");
            z = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            x > 0
            -----*/
            System.out.println("Running _master_state 2");
            boolean _expression_result = x > 0;
            if (_expression_result) _master_state_nxt = 4;
            else _master_state_nxt = 3;
        }
        private void _master_state_4() {
            /*------
            _S0 = 0;
            -----*/
            System.out.println("Running _master_state 4");
            _S0 = 0 ;
            _master_state_nxt = 6;
        }
        private void _master_state_6() {
            /*------
            Foreach (t : G.Nodes)
                _S0 += (t.A + t.B)  @ t ;
            -----*/
            System.out.println("Running _master_state 6");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            
            _master_state_nxt = 7;
            _master_should_start_workers = true;
        }
        private void _master_state_7() {
            /*------
            z = _S0;
            -----*/
            System.out.println("Running _master_state 7");
            _S0 = ((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY__S0)).getValue().getValue();
            z = _S0 ;
            _master_state_nxt = 3;
        }
        private void _master_state_3() {
            /*------
            Return z;
            -----*/
            System.out.println("Running _master_state 3");
            _ret_value = z;
            
            _master_should_finish = true;
        }
    }
    
    //----------------------------------------------
    // Main Vertex Class
    //----------------------------------------------
    public static class gps1Vertex
        extends NullEdgeVertex< gps1.VertexData, gps1.MessageData > {
        
        public gps1Vertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
        }
        
        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }
        
        @Override
        public void compute(Iterable<gps1.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 6: _vertex_state_6(_msgs); break;
            }
        }
        private void _vertex_state_6(Iterable<gps1.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t : G.Nodes)
                _S0 += (t.A + t.B)  @ t ;
            -----*/
            
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__S0,new IntSumGlobalObject((_this.A + _this.B)));
        }
    } // end of Vertex
    
    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class gps1VertexFactory
        extends NullEdgeVertexFactory< gps1.VertexData, gps1.MessageData > {
        @Override
        public NullEdgeVertex< gps1.VertexData, gps1.MessageData > newInstance(CommandLine line) {
            return new gps1Vertex(line);
        }
    } // end of VertexFactory
    
    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // node properties
        int A;
        int B;
        
        @Override
        public int numBytes() {return 8;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(A);
            IOB.putInt(B);
        }
        @Override
        public void read(IoBuffer IOB) {
            A= IOB.getInt();
            B= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            A= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            B= Utils.byteArrayToIntBigEndian(_BA, _idx + 4);
            return 8;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 8);
            return 8;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "" + "A: " + A + "\tB: " + B;
        }
    } // end of vertex-data
    
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
        
        @Override
        public int numBytes() {
            //for empty messages (signaling only)
            return 1; 
        }
        @Override
        public void write(IoBuffer IOB) {
            IOB.put(m_type);
            //for empty messages (signaling only)
        }
        @Override
        public void read(IoBuffer IOB) {
            m_type = IOB.get();
            //for empty messages (signaling only)
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            m_type = _BA[_idx];
            //for empty messages (signaling only)
            return 1;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            byte m_type = IOB.get();
            //for empty messages (signaling only)
            return 1;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
            //do nothing
        }
        
    } // end of message-data
    
}
