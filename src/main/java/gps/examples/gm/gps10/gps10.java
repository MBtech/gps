package gps.examples.gm.gps10;
import gps.*;
import gps.examples.gm.gps10.gps10.MessageData;
import gps.examples.gm.gps10.gps10.VertexData;
import gps.examples.gm.gps10.gps10.gps10Master;
import gps.examples.gm.gps10.gps10.gps10Vertex;
import gps.examples.gm.gps10.gps10.gps10VertexFactory;
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

public class gps10{
    
    // Keys for shared_variables 
    
    public static class gps10Master extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;
        
        public gps10Master (CommandLine line) {
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
                    case 8: _master_state_8(); break;
                    case 9: _master_state_9(); break;
                    case 5: _master_state_5(); break;
                    case 6: _master_state_6(); break;
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
            -----*/
            System.out.println("Running _master_state 0");
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0._in_degree = 0;
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
            Foreach (t0 : G.Nodes)
            {
                Foreach (u1 : t0.Nbrs)
                {
                    u1._in_degree += 1 @ t0 ;
                }
            }
            -----*/
            System.out.println("Running _master_state 4");
            getGlobalObjectsMap().clearNonDefaultObjects();
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
            Foreach (u1 : t0.Nbrs)
            {
                u1._in_degree += 1 @ t0 ;
            }
            -----*/
            System.out.println("Running _master_state 9");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            
            _master_state_nxt = 5;
            _master_should_start_workers = true;
        }
        private void _master_state_5() {
            /*------
            -----*/
            System.out.println("Running _master_state 5");
            _master_state_nxt = 6;
        }
        private void _master_state_6() {
            /*------
            Foreach (n : G.Nodes)
            {
                n.A = n._in_degree;
            }
            -----*/
            System.out.println("Running _master_state 6");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            
            _master_state_nxt = 7;
            _master_should_start_workers = true;
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
    public static class gps10Vertex
        extends NullEdgeVertex< gps10.VertexData, gps10.MessageData > {
        
        public gps10Vertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }
        
        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }
        
        @Override
        public void compute(Iterable<gps10.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 4: _vertex_state_4(_msgs); break;
                case 9: _vertex_state_9(_msgs); break;
                case 6: _vertex_state_6(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<gps10.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0._in_degree = 0;
            }
            -----*/
            
            {
                _this._in_degree = 0 ;
            }
        }
        private void _vertex_state_4(Iterable<gps10.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t0 : G.Nodes)
            {
                Foreach (u1 : t0.Nbrs)
                {
                    u1._in_degree += 1 @ t0 ;
                }
            }
            -----*/
            
            {
                
                // Sending messages
                MessageData _msg = new MessageData((byte) 0);
                sendMessages(getNeighborIds(), _msg);
                
            }
        }
        private void _vertex_state_9(Iterable<gps10.MessageData> _msgs) {
            VertexData _this = getValue();
            
            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                Foreach (u1 : t0.Nbrs)
                {
                    u1._in_degree += 1 @ t0 ;
                }
                -----*/
                _this._in_degree = _this._in_degree + (1);
            }
            
        }
        private void _vertex_state_6(Iterable<gps10.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (n : G.Nodes)
            {
                n.A = n._in_degree;
            }
            -----*/
            
            {
                _this.A = _this._in_degree ;
            }
        }
    } // end of Vertex
    
    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class gps10VertexFactory
        extends NullEdgeVertexFactory< gps10.VertexData, gps10.MessageData > {
        @Override
        public NullEdgeVertex< gps10.VertexData, gps10.MessageData > newInstance(CommandLine line) {
            return new gps10Vertex(line);
        }
    } // end of VertexFactory
    
    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // node properties
        int A;
        int _in_degree;
        
        @Override
        public int numBytes() {return 8;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(A);
            IOB.putInt(_in_degree);
        }
        @Override
        public void read(IoBuffer IOB) {
            A= IOB.getInt();
            _in_degree= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            A= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            _in_degree= Utils.byteArrayToIntBigEndian(_BA, _idx + 4);
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
            return "" + "A: " + A + "\t_in_degree: " + _in_degree;
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
	public static class JobConfiguration extends GPSJobConfiguration {

		public Class<?> getMasterClass() {
			return gps10Master.class;
		}

		@Override
		public Class<?> getVertexFactoryClass() {
			return gps10VertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return gps10Vertex.class;
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
