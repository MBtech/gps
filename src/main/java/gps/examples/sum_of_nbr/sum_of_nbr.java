package gps.examples.sum_of_nbr;
import gps.*;
import gps.examples.sumofneighbors.SumOfNeighborsShortAndFloatValuesVertex;
import gps.examples.sumofneighbors.SumOfNeighborsShortAndFloatValuesVertex.SumOfNeighborsShortAndFloatValuesVertexFactory;
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

public class sum_of_nbr{
    
    // Keys for shared_variables 
    
    public static class sum_of_nbrMaster extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;
        
        public sum_of_nbrMaster (CommandLine line) {
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
            Foreach (t0 : G.Nodes)
            {
                t0.I =  (Int ) .Rand(100000);
                t0.F =  (Float ) (.Uniform() * 1000000) ;
                Foreach (t : t0.Nbrs)
                {
                    t.ISum += t0.I @ t0 ;
                    t.FSum += t0.F @ t0 ;
                }
            }
            -----*/
            System.out.println("Running _master_state 2");
            getGlobalObjectsMap().clearNonDefaultObjects();
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
            //Receive Loops
            Foreach (t : t0.Nbrs)
            {
                t.ISum += t0.I @ t0 ;
                t.FSum += t0.F @ t0 ;
            }
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
    public static class sum_of_nbrVertex
        extends NullEdgeVertex< sum_of_nbr.VertexData, sum_of_nbr.MessageData > {
        
        public sum_of_nbrVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }
        
        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }
        
        @Override
        public void compute(Iterable<sum_of_nbr.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 5: _vertex_state_5(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<sum_of_nbr.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0.I =  (Int ) .Rand(100000);
                t0.F =  (Float ) (.Uniform() * 1000000) ;
                Foreach (t : t0.Nbrs)
                {
                    t.ISum += t0.I @ t0 ;
                    t.FSum += t0.F @ t0 ;
                }
            }
            -----*/
            
            {
                _this.I = (int)((new java.util.Random()).nextInt(100000)) ;
                System.out.println("vertex " + getId() + " has value I: " + _this.I);
                _this.F = (float)((new java.util.Random()).nextDouble() * 1000000) ;
                System.out.println("vertex " + getId() + " has value F: " + _this.F);
                // Sending messages
                MessageData _msg = new MessageData((byte) 0);
                _msg.i0 = _this.I;
                _msg.f0 = _this.F;
                sendMessages(getNeighborIds(), _msg);
                
            }
        }
        private void _vertex_state_5(Iterable<sum_of_nbr.MessageData> _msgs) {
            VertexData _this = getValue();
            
            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
            Foreach (t : t0.Nbrs)
            {
                t.ISum += t0.I @ t0 ;
                t.FSum += t0.F @ t0 ;
            }
                -----*/
                int _remote_I = _msg.i0;
                float _remote_F = _msg.f0;
                _this.ISum = _this.ISum + (_remote_I);
                _this.FSum = _this.FSum + (_remote_F);
            }
            
        }
    } // end of Vertex
    
    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class sum_of_nbrVertexFactory
        extends NullEdgeVertexFactory< sum_of_nbr.VertexData, sum_of_nbr.MessageData > {
        @Override
        public NullEdgeVertex< sum_of_nbr.VertexData, sum_of_nbr.MessageData > newInstance(CommandLine line) {
            return new sum_of_nbrVertex(line);
        }
    } // end of VertexFactory
    
    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // node properties
        int I;
        int ISum;
        float F;
        float FSum;
        
        @Override
        public int numBytes() {return 16;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(I);
            IOB.putInt(ISum);
            IOB.putFloat(F);
            IOB.putFloat(FSum);
        }
        @Override
        public void read(IoBuffer IOB) {
            I= IOB.getInt();
            ISum= IOB.getInt();
            F= IOB.getFloat();
            FSum= IOB.getFloat();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            I= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            ISum= Utils.byteArrayToIntBigEndian(_BA, _idx + 4);
            F= Utils.byteArrayToFloatBigEndian(_BA, _idx + 8);
            FSum= Utils.byteArrayToFloatBigEndian(_BA, _idx + 12);
            return 16;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 16);
            return 16;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "" + "I: " + I + "\tISum: " + ISum + "\tF: " + F + "\tFSum: " + FSum;
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
            if (m_type == 0) return (1+8); // type + data
            //for empty messages (signaling only)
            return 1; 
        }
        @Override
        public void write(IoBuffer IOB) {
            IOB.put(m_type);
            if (m_type == 0) {
                IOB.putInt(i0);
                IOB.putFloat(f0);
            }
            //for empty messages (signaling only)
        }
        @Override
        public void read(IoBuffer IOB) {
            m_type = IOB.get();
            if (m_type == 0) {
                i0= IOB.getInt();
                f0= IOB.getFloat();
            }
            //for empty messages (signaling only)
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            m_type = _BA[_idx];
            if (m_type == 0) {
                i0= Utils.byteArrayToIntBigEndian(_BA, _idx + 1);
                f0= Utils.byteArrayToFloatBigEndian(_BA, _idx + 5);
                return 1 + 8;
            }
            //for empty messages (signaling only)
            return 1;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            byte m_type = IOB.get();
            if (m_type == 0) {
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
    
	public static class JobConfiguration extends GPSJobConfiguration {

		@Override 
		public Class<?> getMasterClass() {
			return sum_of_nbrMaster.class;
		}

		@Override
		public Class<?> getVertexFactoryClass() {
			return sum_of_nbrVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return sum_of_nbrVertex.class;
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
