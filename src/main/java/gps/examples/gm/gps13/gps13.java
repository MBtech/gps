package gps.examples.gm.gps13;
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

public class gps13{

    // Keys for shared_variables 
    private static final String KEY_root = "root";

    public static class gps13Master extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public gps13Master (CommandLine line) {
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
                    case 5: _master_state_5(); break;
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
            -----*/
            System.out.println("Running _master_state 0");
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (n : G.Nodes)
            {
                If (n.A > 0)
                {
                    k = root;
                    k.B += n.A @ n ;
                }
            }
            -----*/
            System.out.println("Running _master_state 2");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_root,new IntOverwriteGlobalObject(root));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 4;
            _master_should_start_workers = true;
        }
        private void _master_state_4() {
            /*------
            -----*/
            System.out.println("Running _master_state 4");
            _master_state_nxt = 5;
        }
        private void _master_state_5() {
            /*------
            //Receive Random Write Sent
            k.B += n.A @ n ;
            -----*/
            System.out.println("Running _master_state 5");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 3;
            _master_should_start_workers = true;
        }
        private void _master_state_3() {
            /*------
            -----*/
            System.out.println("Running _master_state 3");

            _master_should_finish = true;
        }
    }

    //----------------------------------------------
    // Main Vertex Class
    //----------------------------------------------
    public static class gps13Vertex
        extends NullEdgeVertex< gps13.VertexData, gps13.MessageData > {

        public gps13Vertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData(id);
        }

        @Override
        public void compute(Iterable<gps13.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 5: _vertex_state_5(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<gps13.MessageData> _msgs) {
            VertexData _this = getValue();
            int root = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_root)).getValue().getValue();
            int k;
            /*------
            Foreach (n : G.Nodes)
            {
                If (n.A > 0)
                {
                    k = root;
                    k.B += n.A @ n ;
                }
            }
            -----*/

            {
                if (_this.A > 0)
                {

                    MessageData _msg_k = new MessageData((byte) 0);

                    k = root ;
                    _msg_k.i0 = _this.A;

                    sendMessage(k,_msg_k);
                }
            }
        }
        private void _vertex_state_5(Iterable<gps13.MessageData> _msgs) {
            VertexData _this = getValue();

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Random Write)
                {
                    k.B += n.A @ n ;
                }
                -----*/
                int _remote_A = _msg.i0;
                _this.B = _this.B + (_remote_A);
            }

        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class gps13VertexFactory
        extends NullEdgeVertexFactory< gps13.VertexData, gps13.MessageData > {
        @Override
        public NullEdgeVertex< gps13.VertexData, gps13.MessageData > newInstance(CommandLine line) {
            return new gps13Vertex(line);
        }
    } // end of VertexFactory

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // properties
        int A;
        int B;

        public VertexData(int id) {
        	if (id == 1) {
        		this.A = 100;
        	}
        	this.B = id;
        }

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


    // job description for the system
    public static class JobConfiguration extends GPSJobConfiguration {
        @Override
        public Class<?> getMasterClass() {
            return gps13Master.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return gps13VertexFactory.class;
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
			return gps13Vertex.class;
		}
    }
}
