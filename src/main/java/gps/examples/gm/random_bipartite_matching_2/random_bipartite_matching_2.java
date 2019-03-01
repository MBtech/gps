package gps.examples.gm.random_bipartite_matching_2;
import gps.*;
import gps.examples.handwrittenrandombipartitematching.RandomBipartiteMatchingVertex.VertexData;
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

public class random_bipartite_matching_2{

    // Keys for shared_variables 
    private static final String KEY_count = "count";

    public static class random_bipartite_matching_2Master extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public random_bipartite_matching_2Master (CommandLine line) {
            // parse command-line arguments (if any)
            java.util.HashMap<String,String> arg_map = new java.util.HashMap<String,String>();
            gps.node.Utils.parseOtherOptions(line, arg_map);

            if (arg_map.containsKey("max")) {
                String s = arg_map.get("max");
                max = Integer.parseInt(s);
            }
        }

        //save output
        public void writeOutput(BufferedWriter bw) throws IOException {
            bw.write("_ret_value:\t" + _ret_value + "\n");
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private int max;
        private int count;
        private int iterationCnt;
        private int _ret_value; // the final return value of the procedure
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
                    case 8: _master_state_8(); break;
                    case 20: _master_state_20(); break;
                    case 14: _master_state_14(); break;
                    case 21: _master_state_21(); break;
                    case 16: _master_state_16(); break;
                    case 10: _master_state_10(); break;
                    case 18: _master_state_18(); break;
                    case 12: _master_state_12(); break;
                    case 4: _master_state_4(); break;
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
            count = 0;
            iterationCnt = 0;
            -----*/
            System.out.println("Running _master_state 0");

            count = 0 ;
            iterationCnt = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0.Match = NIL;
                t0.Suitor = NIL;
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

            _master_state_nxt = 8;
        }
        private void _master_state_8() {
            /*------
            -----*/
            System.out.println("Running _master_state 8");

            _master_state_nxt = 20;
        }
        private void _master_state_20() {
            /*------
            //Receive Random Write Sent
            r.Match = k;
            Foreach (n : G.Nodes)
            {
                If (n.isLeft && (n.Match == NIL) )
                {
                    Foreach (t : n.Nbrs)
                    {
                        If (t.Match == NIL)
                        {
                            t.Suitor = n;
                        }
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 20");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            getGlobalObjectsMap().putOrUpdateGlobalObject("_is_first_4",new BooleanOverwriteGlobalObject(_is_first_4));

            _master_state_nxt = 14;
            _master_should_start_workers = true;
        }
        private void _master_state_14() {
            /*------
            iterationCnt = iterationCnt + 1;
            -----*/
            System.out.println("Running _master_state 14");

            if (!_is_first_4) {
                iterationCnt = iterationCnt + 1 ;
            }
            _master_state_nxt = 21;
        }
        private void _master_state_21() {
            /*------
            -----*/
            System.out.println("Running _master_state 21");
            // Intra-Loop Merged
            if (_is_first_4) _master_state_nxt = 16;
            else _master_state_nxt = 4;
            _is_first_4 = false;

        }
        private void _master_state_16() {
            /*------
            //Receive Nested Loop
            Foreach (t : n.Nbrs)
            {
                If (t.Match == NIL)
                {
                    t.Suitor = n;
                }
            }
            Foreach (t2 : G.Nodes)
            {
                If (!t2.isLeft && (t2.Match == NIL) )
                {
                    If (t2.Suitor != NIL)
                    {
                        n3 = t2.Suitor;
                        n3.Match = t2;
                        n3.Suitor = t2;
                        t2.Suitor = NIL;
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 16");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 10;
            _master_should_start_workers = true;
        }
        private void _master_state_10() {
            /*------
            -----*/
            System.out.println("Running _master_state 10");

            _master_state_nxt = 18;
        }
        private void _master_state_18() {
            /*------
            //Receive Random Write Sent
            n3.Match = t2;
            n3.Suitor = t2;
            //Receive Random Write Sent
            n3.Match = t2;
            n3.Suitor = t2;
            Foreach (k : G.Nodes)
            {
                If (k.isLeft && (k.Suitor != NIL) )
                {
                    r = k.Match;
                    r.Match = k;
                    k.Suitor = NIL;
                    count += 1 @ k ;
                }
            }
            -----*/
            System.out.println("Running _master_state 18");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_count,new IntSumGlobalObject(0));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 12;
            _master_should_start_workers = true;
        }
        private void _master_state_12() {
            /*------
            -----*/
            System.out.println("Running _master_state 12");
            count = count+((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY_count)).getValue().getValue();

            _master_state_nxt = 20;
        }
        private void _master_state_4() {
            /*------
            iterationCnt < max
            -----*/
            System.out.println("Running _master_state 4");
            // Do-While(...)

            boolean _expression_result = iterationCnt < max;
            if (_expression_result) _master_state_nxt = 8;
            else _master_state_nxt = 7;

            if (!_expression_result) _is_first_4=true; // reset is_first


        }
        private void _master_state_7() {
            /*------
            Return count;
            -----*/
            System.out.println("Running _master_state 7");

            _ret_value = count;

            _master_should_finish = true;
        }
    }

    //----------------------------------------------
    // Main Vertex Class
    //----------------------------------------------
    public static class random_bipartite_matching_2Vertex
        extends NullEdgeVertex< random_bipartite_matching_2.VertexData, random_bipartite_matching_2.MessageData > {

        public random_bipartite_matching_2Vertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData((id % 2) == 0);
        }

        @Override
        public void compute(Iterable<random_bipartite_matching_2.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 20: _vertex_state_20(_msgs); break;
                case 16: _vertex_state_16(_msgs); break;
                case 18: _vertex_state_18(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<random_bipartite_matching_2.MessageData> _msgs) {
            VertexData _this = getValue();
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0.Match = NIL;
                t0.Suitor = NIL;
            }
            -----*/

            {
                _this.Match = (-1) ;
                _this.Suitor = (-1) ;
            }
        }
        private void _vertex_state_20(Iterable<random_bipartite_matching_2.MessageData> _msgs) {
            VertexData _this = getValue();
            boolean _is_first_4 = ((BooleanOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("_is_first_4")).getValue().getValue();

            if (!_is_first_4) {
                // Begin msg receive
                for(MessageData _msg : _msgs) {
                    /*------
                    (Random Write)
                    {
                        r.Match = k;
                    }
                    -----*/
                    int k = _msg.i0;
                    _this.Match = k ;
                }
            }

            /*------
            //Receive Random Write Sent
            r.Match = k;
            Foreach (n : G.Nodes)
            {
                If (n.isLeft && (n.Match == NIL) )
                {
                    Foreach (t : n.Nbrs)
                    {
                        If (t.Match == NIL)
                        {
                            t.Suitor = n;
                        }
                    }
                }
            }
            -----*/

            {
                if (_this.isLeft && (_this.Match == (-1)))
                {
                    // Sending messages to all neighbors
                    MessageData _msg = new MessageData((byte) 0);
                    _msg.i0 = getId();
                    sendMessages(getNeighborIds(), _msg);
                }
            }
        }
        private void _vertex_state_16(Iterable<random_bipartite_matching_2.MessageData> _msgs) {
            VertexData _this = getValue();
            int n3;

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                Foreach (t : n.Nbrs)
                {
                    If (t.Match == NIL)
                    {
                        t.Suitor = n;
                    }
                }
                -----*/
                int n = _msg.i0;
                if (_this.Match == (-1))
                {
                    _this.Suitor = n ;
                }
            }

            /*------
            //Receive Nested Loop
            Foreach (t : n.Nbrs)
            {
                If (t.Match == NIL)
                {
                    t.Suitor = n;
                }
            }
            Foreach (t2 : G.Nodes)
            {
                If (!t2.isLeft && (t2.Match == NIL) )
                {
                    If (t2.Suitor != NIL)
                    {
                        n3 = t2.Suitor;
                        n3.Match = t2;
                        n3.Suitor = t2;
                        t2.Suitor = NIL;
                    }
                }
            }
            -----*/

            {
                if ( !_this.isLeft && (_this.Match == (-1)))
                {
                    if (_this.Suitor != (-1))
                    {

                        MessageData _msg_n3 = new MessageData((byte) 1);

                        n3 = _this.Suitor ;
                        _msg_n3.i0 = getId();
                        _msg_n3.i0 = getId();
                        _this.Suitor = (-1) ;

                        sendMessage(n3,_msg_n3);
                    }
                }
            }
        }
        private void _vertex_state_18(Iterable<random_bipartite_matching_2.MessageData> _msgs) {
            VertexData _this = getValue();
            int r;

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Random Write)
                {
                    n3.Match = t2;
                    n3.Suitor = t2;
                }
                -----*/
                if (_msg.m_type == 1) {
                    int t2 = _msg.i0;
                    _this.Match = t2 ;
                    _this.Suitor = t2 ;
                }
                /*------
                (Random Write)
                {
                    n3.Match = t2;
                    n3.Suitor = t2;
                }
                -----*/
                if (_msg.m_type == 1) {
                    int t2 = _msg.i0;
                    _this.Match = t2 ;
                    _this.Suitor = t2 ;
                }
            }

            /*------
            //Receive Random Write Sent
            n3.Match = t2;
            n3.Suitor = t2;
            //Receive Random Write Sent
            n3.Match = t2;
            n3.Suitor = t2;
            Foreach (k : G.Nodes)
            {
                If (k.isLeft && (k.Suitor != NIL) )
                {
                    r = k.Match;
                    r.Match = k;
                    k.Suitor = NIL;
                    count += 1 @ k ;
                }
            }
            -----*/

            {
                if (_this.isLeft && (_this.Suitor != (-1)))
                {

                    MessageData _msg_r = new MessageData((byte) 0);

                    r = _this.Match ;
                    _msg_r.i0 = getId();
                    _this.Suitor = (-1) ;
                    getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_count,new IntSumGlobalObject(1));

                    sendMessage(r,_msg_r);
                }
            }
        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class random_bipartite_matching_2VertexFactory
        extends NullEdgeVertexFactory< random_bipartite_matching_2.VertexData, random_bipartite_matching_2.MessageData > {
        @Override
        public NullEdgeVertex< random_bipartite_matching_2.VertexData, random_bipartite_matching_2.MessageData > newInstance(CommandLine line) {
            return new random_bipartite_matching_2Vertex(line);
        }
    } // end of VertexFactory

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // properties
        boolean isLeft;
        int Match;
        int Suitor;

        public VertexData(boolean isLeft) {
        	this.isLeft = isLeft;
		}

        @Override
        public int numBytes() {return 9;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.put(isLeft?(byte)1:(byte)0);
            IOB.putInt(Match);
            IOB.putInt(Suitor);
        }
        @Override
        public void read(IoBuffer IOB) {
            isLeft= IOB.get()==0?false:true;
            Match= IOB.getInt();
            Suitor= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            isLeft= Utils.byteArrayToBooleanBigEndian(_BA, _idx + 0);
            Match= Utils.byteArrayToIntBigEndian(_BA, _idx + 1);
            Suitor= Utils.byteArrayToIntBigEndian(_BA, _idx + 5);
            return 9;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 9);
            return 9;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "" + "Match: " + Match;
        }
        //Input Data Parsing
        @Override
        public void read(String inputString) {
            this.isLeft = Boolean.parseBoolean(inputString);
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

        @Override
        public int numBytes() {
            if (m_type == 2) return (1+4); // type + data
            else if (m_type == 1) return (1+4); // type + data
            //for empty messages (signaling only)
            return 1; 
        }
        @Override
        public void write(IoBuffer IOB) {
            IOB.put(m_type);
            if (m_type == 2) {
                IOB.putInt(i0);
            }
            else if (m_type == 1) {
                IOB.putInt(i0);
            }
            //for empty messages (signaling only)
        }
        @Override
        public void read(IoBuffer IOB) {
            m_type = IOB.get();
            if (m_type == 2) {
                i0= IOB.getInt();
            }
            else if (m_type == 1) {
                i0= IOB.getInt();
            }
            //for empty messages (signaling only)
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            m_type = _BA[_idx];
            if (m_type == 2) {
                i0= Utils.byteArrayToIntBigEndian(_BA, _idx + 1);
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
            IOB.get(_BA, _idx, 1);
            byte m_type = _BA[_idx];
            if (m_type == 2) {
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


    // job description for the system
    public static class JobConfiguration extends GPSJobConfiguration {
        @Override
        public Class<?> getMasterClass() {
            return random_bipartite_matching_2Master.class;
        }
        @Override
        public Class<?> getVertexClass() {
            return random_bipartite_matching_2Vertex.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return random_bipartite_matching_2VertexFactory.class;
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
//        @Override
//        public boolean hasVertexValuesInInput() {
//            return true;
//        }
    }
}
