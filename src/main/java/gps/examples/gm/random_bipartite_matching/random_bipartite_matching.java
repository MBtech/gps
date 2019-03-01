package gps.examples.gm.random_bipartite_matching;
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

public class random_bipartite_matching{

    // Keys for shared_variables 
    private static final String KEY_count = "count";
    private static final String KEY_finished = "finished";

    public static class random_bipartite_matchingMaster extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public random_bipartite_matchingMaster (CommandLine line) {
            // parse command-line arguments (if any)
            java.util.HashMap<String,String> arg_map = new java.util.HashMap<String,String>();
            gps.node.Utils.parseOtherOptions(line, arg_map);

        }

        //save output
        public void writeOutput(BufferedWriter bw) throws IOException {
            bw.write("_ret_value:\t" + _ret_value + "\n");
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private int count;
        private boolean finished;
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
                    case 4: _master_state_4(); break;
                    case 5: _master_state_5(); break;
                    case 20: _master_state_20(); break;
                    case 14: _master_state_14(); break;
                    case 21: _master_state_21(); break;
                    case 16: _master_state_16(); break;
                    case 10: _master_state_10(); break;
                    case 18: _master_state_18(); break;
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
            count = 0;
            finished = False;
            -----*/
            System.out.println("Running _master_state 0");

            count = 0 ;
            finished = false ;
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

            _master_state_nxt = 4;
        }
        private void _master_state_4() {
            /*------
            !finished
            -----*/
            System.out.println("Running _master_state 4");
            // While (...)

            boolean _expression_result =  !finished;
            if (_expression_result) _master_state_nxt = 5;
            else _master_state_nxt = 7;

            if (!_expression_result) _is_first_4=true; // reset is_first


        }
        private void _master_state_5() {
            /*------
            finished = True;
            -----*/
            System.out.println("Running _master_state 5");

            finished = true ;
            _master_state_nxt = 20;
        }
        private void _master_state_20() {
            /*------
            //Receive Random Write Sent
            t5.Match = n4;
            Foreach (n : G.Nodes)
            {
                If (n.isLeft && (n.Match == NIL) )
                {
                    Foreach (t : n.Nbrs)
                    {
                        If (t.Match == NIL)
                        {
                            t.Suitor = n;
                            finished &= False @ n ;
                        }
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 20");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_finished,new BooleanANDGlobalObject(true));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));
            getGlobalObjectsMap().putOrUpdateGlobalObject("_is_first_4",new BooleanOverwriteGlobalObject(_is_first_4));

            _master_state_nxt = 14;
            _master_should_start_workers = true;
        }
        private void _master_state_14() {
            /*------
            -----*/
            System.out.println("Running _master_state 14");
            finished = finished&&((BooleanANDGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY_finished)).getValue().getValue();

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
                    finished &= False @ n ;
                }
            }
            Foreach (t2 : G.Nodes)
            {
                If (!t2.isLeft && (t2.Match == NIL) )
                {
                    If (t2.Suitor != NIL)
                    {
                        n3 = t2.Suitor;
                        n3.Suitor = t2;
                        t2.Suitor = NIL;
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 16");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_finished,new BooleanANDGlobalObject(true));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 10;
            _master_should_start_workers = true;
        }
        private void _master_state_10() {
            /*------
            -----*/
            System.out.println("Running _master_state 10");
            finished = finished&&((BooleanANDGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY_finished)).getValue().getValue();

            _master_state_nxt = 18;
        }
        private void _master_state_18() {
            /*------
            //Receive Random Write Sent
            n3.Suitor = t2;
            Foreach (n4 : G.Nodes)
            {
                If (n4.isLeft && (n4.Match == NIL) )
                {
                    If (n4.Suitor != NIL)
                    {
                        t5 = n4.Suitor;
                        n4.Match = t5;
                        t5.Match = n4;
                        count += 1 @ n4 ;
                    }
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
    public static class random_bipartite_matchingVertex
        extends NullEdgeVertex< random_bipartite_matching.VertexData, random_bipartite_matching.MessageData > {

        public random_bipartite_matchingVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData();
        }

        @Override
        public void compute(Iterable<random_bipartite_matching.MessageData> _msgs, int _superStepNo) {
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
        private void _vertex_state_2(Iterable<random_bipartite_matching.MessageData> _msgs) {
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
        private void _vertex_state_20(Iterable<random_bipartite_matching.MessageData> _msgs) {
            VertexData _this = getValue();
            boolean _is_first_4 = ((BooleanOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("_is_first_4")).getValue().getValue();

            if (!_is_first_4) {
                // Begin msg receive
                for(MessageData _msg : _msgs) {
                    /*------
                    (Random Write)
                    {
                        t5.Match = n4;
                    }
                    -----*/
                    int n4 = _msg.i0;
                    _this.Match = n4 ;
                }
            }

            /*------
            //Receive Random Write Sent
            t5.Match = n4;
            Foreach (n : G.Nodes)
            {
                If (n.isLeft && (n.Match == NIL) )
                {
                    Foreach (t : n.Nbrs)
                    {
                        If (t.Match == NIL)
                        {
                            t.Suitor = n;
                            finished &= False @ n ;
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
        private void _vertex_state_16(Iterable<random_bipartite_matching.MessageData> _msgs) {
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
                        finished &= False @ n ;
                    }
                }
                -----*/
                int n = _msg.i0;
                if (_this.Match == (-1))
                {
                    _this.Suitor = n ;
                    getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_finished,new BooleanANDGlobalObject(false));
                }
            }

            /*------
            //Receive Nested Loop
            Foreach (t : n.Nbrs)
            {
                If (t.Match == NIL)
                {
                    t.Suitor = n;
                    finished &= False @ n ;
                }
            }
            Foreach (t2 : G.Nodes)
            {
                If (!t2.isLeft && (t2.Match == NIL) )
                {
                    If (t2.Suitor != NIL)
                    {
                        n3 = t2.Suitor;
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

                        MessageData _msg_n3 = new MessageData((byte) 0);

                        n3 = _this.Suitor ;
                        _msg_n3.i0 = getId();
                        _this.Suitor = (-1) ;

                        sendMessage(n3,_msg_n3);
                    }
                }
            }
        }
        private void _vertex_state_18(Iterable<random_bipartite_matching.MessageData> _msgs) {
            VertexData _this = getValue();
            int t5;

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Random Write)
                {
                    n3.Suitor = t2;
                }
                -----*/
                int t2 = _msg.i0;
                _this.Suitor = t2 ;
            }

            /*------
            //Receive Random Write Sent
            n3.Suitor = t2;
            Foreach (n4 : G.Nodes)
            {
                If (n4.isLeft && (n4.Match == NIL) )
                {
                    If (n4.Suitor != NIL)
                    {
                        t5 = n4.Suitor;
                        n4.Match = t5;
                        t5.Match = n4;
                        count += 1 @ n4 ;
                    }
                }
            }
            -----*/

            {
                if (_this.isLeft && (_this.Match == (-1)))
                {
                    if (_this.Suitor != (-1))
                    {

                        MessageData _msg_t5 = new MessageData((byte) 0);

                        t5 = _this.Suitor ;
                        _this.Match = t5 ;
                        _msg_t5.i0 = getId();
                        getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_count,new IntSumGlobalObject(1));

                        sendMessage(t5,_msg_t5);
                    }
                }
            }
        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class random_bipartite_matchingVertexFactory
        extends NullEdgeVertexFactory< random_bipartite_matching.VertexData, random_bipartite_matching.MessageData > {
        @Override
        public NullEdgeVertex< random_bipartite_matching.VertexData, random_bipartite_matching.MessageData > newInstance(CommandLine line) {
            return new random_bipartite_matchingVertex(line);
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
            return random_bipartite_matchingMaster.class;
        }
        @Override
        public Class<?> getVertexClass() {
            return random_bipartite_matchingVertex.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return random_bipartite_matchingVertexFactory.class;
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
        public boolean hasVertexValuesInInput() {
            return true;
        }
    }
}
