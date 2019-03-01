package gps.examples.gm.gps4;
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

public class gps4{
    
    // Keys for shared_variables 
    private static final String KEY_x = "x";
    
    public static class gps4Master extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;
        
        public gps4Master (CommandLine line) {
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
        private int x;
        
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
                    case 6: _master_state_6(); break;
                    case 7: _master_state_7(); break;
                    case 3: _master_state_3(); break;
                    case 4: _master_state_4(); break;
                    case 8: _master_state_8(); break;
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
            x = 0;
            -----*/
            System.out.println("Running _master_state 0");
            x = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (s : G.Nodes)
            {
                y =  (Float ) 1;
                Foreach (t : s.Nbrs)
                {
                    t.A +=  (Int ) y @ s ;
                }
            }
            -----*/
            System.out.println("Running _master_state 2");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            
            _master_state_nxt = 6;
            _master_should_start_workers = true;
        }
        private void _master_state_6() {
            /*------
            -----*/
            System.out.println("Running _master_state 6");
            _master_state_nxt = 7;
        }
        private void _master_state_7() {
            /*------
            //Receive Loops
            Foreach (t : s.Nbrs)
            {
                t.A +=  (Int ) y @ s ;
            }
            -----*/
            System.out.println("Running _master_state 7");
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
            Foreach (s0 : G.Nodes)
            {
                Foreach (t1 : s0.Nbrs)
                {
                    t1.A += s0.B + x + 1 @ s0 ;
                }
            }
            -----*/
            System.out.println("Running _master_state 4");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_x,new IntOverwriteGlobalObject(x));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            
            _master_state_nxt = 8;
            _master_should_start_workers = true;
        }
        private void _master_state_8() {
            /*------
            -----*/
            System.out.println("Running _master_state 8");
            _master_state_nxt = 9;
        }
        private void _master_state_9() {
            /*------
            //Receive Loops
            Foreach (t1 : s0.Nbrs)
            {
                t1.A += s0.B + x + 1 @ s0 ;
            }
            -----*/
            System.out.println("Running _master_state 9");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_x,new IntOverwriteGlobalObject(x));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            
            _master_state_nxt = 5;
            _master_should_start_workers = true;
        }
        private void _master_state_5() {
            /*------
            -----*/
            System.out.println("Running _master_state 5");
            
            _master_should_finish = true;
        }
    }
    
    //----------------------------------------------
    // Main Vertex Class
    //----------------------------------------------
    public static class gps4Vertex
        extends NullEdgeVertex< gps4.VertexData, gps4.MessageData > {
        
        public gps4Vertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
        }
        
        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }
        
        @Override
        public void compute(Iterable<gps4.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 7: _vertex_state_7(_msgs); break;
                case 4: _vertex_state_4(_msgs); break;
                case 9: _vertex_state_9(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<gps4.MessageData> _msgs) {
            VertexData _this = getValue();
            float y;
            /*------
            Foreach (s : G.Nodes)
            {
                y =  (Float ) 1;
                Foreach (t : s.Nbrs)
                {
                    t.A +=  (Int ) y @ s ;
                }
            }
            -----*/
            
            {
                y = (float)1 ;
                
                // Sending messages
                MessageData _msg = new MessageData((byte) 0);
                _msg.f0 = y;
                sendMessages(getNeighborIds(), _msg);
                
            }
        }
        private void _vertex_state_7(Iterable<gps4.MessageData> _msgs) {
            VertexData _this = getValue();
            float y;
            
            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
            Foreach (t : s.Nbrs)
            {
                t.A +=  (Int ) y @ s ;
            }
                -----*/
                y = _msg.f0;
                _this.A = _this.A + ((int)y);
            }
            
        }
        private void _vertex_state_4(Iterable<gps4.MessageData> _msgs) {
            VertexData _this = getValue();
            int x = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_x)).getValue().getValue();
            /*------
            Foreach (s0 : G.Nodes)
            {
                Foreach (t1 : s0.Nbrs)
                {
                    t1.A += s0.B + x + 1 @ s0 ;
                }
            }
            -----*/
            
            {
                
                // Sending messages
                MessageData _msg = new MessageData((byte) 1);
                _msg.i0 = _this.B;
                sendMessages(getNeighborIds(), _msg);
                
            }
        }
        private void _vertex_state_9(Iterable<gps4.MessageData> _msgs) {
            VertexData _this = getValue();
            int x = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_x)).getValue().getValue();
            
            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
            Foreach (t1 : s0.Nbrs)
            {
                t1.A += s0.B + x + 1 @ s0 ;
            }
                -----*/
                int _remote_B = _msg.i0;
                _this.A = _this.A + (_remote_B + x + 1);
            }
            
        }
    } // end of Vertex
    
    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class gps4VertexFactory
        extends NullEdgeVertexFactory< gps4.VertexData, gps4.MessageData > {
        @Override
        public NullEdgeVertex< gps4.VertexData, gps4.MessageData > newInstance(CommandLine line) {
            return new gps4Vertex(line);
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
        int i0;
        float f0;
        
        @Override
        public int numBytes() {
            if (m_type == 0) return (1+4); // type + data
            else if (m_type == 1) return (1+4); // type + data
            //for empty messages (signaling only)
            return 1; 
        }
        @Override
        public void write(IoBuffer IOB) {
            IOB.put(m_type);
            if (m_type == 0) {
                IOB.putFloat(f0);
            }
            else if (m_type == 1) {
                IOB.putInt(i0);
            }
            //for empty messages (signaling only)
        }
        @Override
        public void read(IoBuffer IOB) {
            m_type = IOB.get();
            if (m_type == 0) {
                f0= IOB.getFloat();
            }
            else if (m_type == 1) {
                i0= IOB.getInt();
            }
            //for empty messages (signaling only)
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            m_type = _BA[_idx];
            if (m_type == 0) {
                f0= Utils.byteArrayToFloatBigEndian(_BA, _idx + 1);
                return 1 + 4;
            }
            else if (m_type == 1) {
                i0= Utils.byteArrayToIntBigEndian(_BA, _idx + 1);
                return 1 + 4;
            }
            //for empty messages (signaling only)
            return 1;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            byte m_type = IOB.get();
            if (m_type == 0) {
                IOB.get(_BA, _idx+1, 4);
                return 1 + 4;
            }
            else if (m_type == 1) {
                IOB.get(_BA, _idx+1, 4);
                return 1 + 4;
            }
            //for empty messages (signaling only)
            return 1;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
            //do nothing
        }
        
    } // end of message-data
    
}
