package gps.examples.gm.gps11;
import gps.*;
import gps.examples.gm.gps11.gps11.MessageData;
import gps.examples.gm.gps11.gps11.VertexData;
import gps.examples.gm.gps11.gps11.gps11Master;
import gps.examples.gm.gps11.gps11.gps11Vertex;
import gps.examples.gm.gps11.gps11.gps11VertexFactory;
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

public class gps11{
    
    // Keys for shared_variables 
    
    public static class gps11Master extends Master {
        // Control fields
        private int     _master_state                = 100000;
        private int     _master_state_nxt            = 100000;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;
        
        public gps11Master (CommandLine line) {
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
                    case 100000: _master_state_100000(); break;
                    case 100001: _master_state_100001(); break;
                    case 0: _master_state_0(); break;
                    case 2: _master_state_2(); break;
                    case 3: _master_state_3(); break;
                    case 4: _master_state_4(); break;
                    case 6: _master_state_6(); break;
                    case 7: _master_state_7(); break;
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
            -----*/
            System.out.println("Running _master_state 0");
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (t0 : G.Nodes)
                t0.A = 0;
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
            Foreach (n : G.Nodes)
            {
                Foreach (t : n.InNbrs)
                    t.A += n.B @ n ;
            }
            -----*/
            System.out.println("Running _master_state 4");
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
            Foreach (t : n.InNbrs)
                t.A += n.B @ n ;
            -----*/
            System.out.println("Running _master_state 7");
            getGlobalObjectsMap().clearNonDefaultObjects();
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
    public static class gps11Vertex
        extends NullEdgeVertex< gps11.VertexData, gps11.MessageData > {
        
        public gps11Vertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }
        
        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }
        
        @Override
        public void compute(Iterable<gps11.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 100000: _vertex_state_100000(_msgs); break;
                case 100001: _vertex_state_100001(_msgs); break;
                case 2: _vertex_state_2(_msgs); break;
                case 4: _vertex_state_4(_msgs); break;
                case 7: _vertex_state_7(_msgs); break;
            }
        }
        private void _vertex_state_100000(Iterable<gps11.MessageData> _msgs) {
            VertexData _this = getValue();
            // Preperation: creating reverse edges
            int _remoteNodeId = getId();
            
            // Sending messages
            MessageData _msg = new MessageData((byte) 0);
            _msg.i0 = _remoteNodeId;
            sendMessages(getNeighborIds(), _msg);
            
        }
        private void _vertex_state_100001(Iterable<gps11.MessageData> _msgs) {
            VertexData _this = getValue();
            //Preperation creating reverse edges
            int i = 0;
            for(MessageData _msg : _msgs) i++;
            _this._revNodeId = new int[i];
            
            i=0;
            for(MessageData _msg : _msgs) {
                int _remoteNodeId = _msg.i0;
                _this._revNodeId[i] = _remoteNodeId;
                i++;
            }
        }
        private void _vertex_state_2(Iterable<gps11.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t0 : G.Nodes)
                t0.A = 0;
            -----*/
            
            _this.A = 0 ;
        }
        private void _vertex_state_4(Iterable<gps11.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (n : G.Nodes)
            {
                Foreach (t : n.InNbrs)
                    t.A += n.B @ n ;
            }
            -----*/
            
            {
                
                // Sending messages
                MessageData _msg = new MessageData((byte) 0);
                _msg.i0 = _this.B;
                sendMessages(_this._revNodeId, _msg);
                
            }
        }
        private void _vertex_state_7(Iterable<gps11.MessageData> _msgs) {
            VertexData _this = getValue();
            
            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                Foreach (t : n.InNbrs)
                    t.A += n.B @ n ;
                -----*/
                int _remote_B = _msg.i0;
                _this.A = _this.A + (_remote_B);
            }
            
        }
    } // end of Vertex
    
    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class gps11VertexFactory
        extends NullEdgeVertexFactory< gps11.VertexData, gps11.MessageData > {
        @Override
        public NullEdgeVertex< gps11.VertexData, gps11.MessageData > newInstance(CommandLine line) {
            return new gps11Vertex(line);
        }
    } // end of VertexFactory
    
    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // node properties
        int A;
        int B = 1;
        int [] _revNodeId; //reverse edges (node IDs) {should this to be marshalled?}
        
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
 
	public static class JobConfiguration extends GPSJobConfiguration {

		public Class<?> getMasterClass() {
			return gps11Master.class;
		}

		@Override
		public Class<?> getVertexFactoryClass() {
			return gps11VertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return gps11Vertex.class;
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
