package gps.examples.gm.gpsPageRank;
import gps.*;
import gps.examples.pagerank.PageRankVertex;
import gps.examples.pagerank.PageRankVertex.PageRankVertexFactory;
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

public class gpsPageRank{
    
    // Keys for shared_variables 
    
    public static class gpsPageRankMaster extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;
        
        public gpsPageRankMaster (CommandLine line) {
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
        private int counter;
        
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
            counter = 0;
            -----*/
            System.out.println("Running _master_state 0");
            counter = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (s : G.Nodes)
            {
                s.tmpSum =  (Double ) 0;
                s.pr = 0.100000;
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
            counter <= 2
            -----*/
            System.out.println("Running _master_state 4");
            // While (...)
            
            boolean _expression_result = counter <= 10;
            if (_expression_result) _master_state_nxt = 5;
            else _master_state_nxt = 7;
            
        }
        private void _master_state_5() {
            /*------
            -----*/
            System.out.println("Running _master_state 5");
            _master_state_nxt = 9;
        }
        private void _master_state_9() {
            /*------
            Foreach (s0 : G.Nodes)
            {
                Foreach (t : s0.Nbrs)
                {
                    t.tmpSum += s0.pr @ s0 ;
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
            //Receive Loops
            Foreach (t : s0.Nbrs)
            {
                t.tmpSum += s0.pr @ s0 ;
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
            Foreach (s1 : G.Nodes)
            {
                s1.pr = 0.850000 * s1.tmpSum;
                s1.tmpSum =  (Double ) 0;
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
            counter = counter + 1;
            -----*/
            System.out.println("Running _master_state 12");
            counter = counter + 1 ;
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
    public static class gpsPageRankVertex
        extends NullEdgeVertex< gpsPageRank.VertexData, gpsPageRank.MessageData > {

        public gpsPageRankVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }

        @Override
        public void compute(Iterable<gpsPageRank.MessageData> _msgs, int _superStepNo) {
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
        private void _vertex_state_2(Iterable<gpsPageRank.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (s : G.Nodes)
            {
                s.tmpSum =  (Double ) 0;
                s.pr = 0.100000;
            }
            -----*/
            
            {
                _this.tmpSum = (double)0 ;
                _this.pr = 0.100000 ;
            }
        }
        private void _vertex_state_9(Iterable<gpsPageRank.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (s0 : G.Nodes)
            {
                Foreach (t : s0.Nbrs)
                {
                    t.tmpSum += s0.pr @ s0 ;
                }
            }
            -----*/
            
            {
                
                // Sending messages
                MessageData _msg = new MessageData((byte) 0);
                _msg.d0 = _this.pr;
                sendMessages(getNeighborIds(), _msg);
                
            }
        }
        private void _vertex_state_14(Iterable<gpsPageRank.MessageData> _msgs) {
            VertexData _this = getValue();
            
            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
            Foreach (t : s0.Nbrs)
            {
                t.tmpSum += s0.pr @ s0 ;
            }
                -----*/
                double _remote_pr = _msg.d0;
                _this.tmpSum = _this.tmpSum + (_remote_pr);
            }
            
        }
        private void _vertex_state_11(Iterable<gpsPageRank.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (s1 : G.Nodes)
            {
                s1.pr = 0.850000 * s1.tmpSum;
                s1.tmpSum =  (Double ) 0;
            }
            -----*/
            
            {
                _this.pr = 0.850000 * _this.tmpSum ;
                _this.tmpSum = (double)0 ;
            }
        }
    } // end of Vertex
    
    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class gpsPageRankVertexFactory
        extends NullEdgeVertexFactory< gpsPageRank.VertexData, gpsPageRank.MessageData > {
        @Override
        public NullEdgeVertex< gpsPageRank.VertexData, gpsPageRank.MessageData > newInstance(CommandLine line) {
            return new gpsPageRankVertex(line);
        }
    } // end of VertexFactory
    
    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // node properties
        double pr;
        double tmpSum;
        
        @Override
        public int numBytes() {return 16;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putDouble(pr);
            IOB.putDouble(tmpSum);
        }
        @Override
        public void read(IoBuffer IOB) {
            pr= IOB.getDouble();
            tmpSum= IOB.getDouble();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            pr= Utils.byteArrayToDoubleBigEndian(_BA, _idx + 0);
            tmpSum= Utils.byteArrayToDoubleBigEndian(_BA, _idx + 8);
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
            return "" + "pr: " + pr + "\ttmpSum: " + tmpSum;
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
        double d0;
        
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
                IOB.putDouble(d0);
            }
            //for empty messages (signaling only)
        }
        @Override
        public void read(IoBuffer IOB) {
            m_type = IOB.get();
            if (m_type == 0) {
                d0= IOB.getDouble();
            }
            //for empty messages (signaling only)
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            m_type = _BA[_idx];
            if (m_type == 0) {
                d0= Utils.byteArrayToDoubleBigEndian(_BA, _idx + 1);
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

		public Class<?> getMasterClass() {
			return gpsPageRankMaster.class;
		}

		@Override
		public Class<?> getVertexFactoryClass() {
			return gpsPageRankVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return gpsPageRankVertex.class;
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
