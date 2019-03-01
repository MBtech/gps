package gps.examples.gm.conductance;
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

public class conduct{

    // Keys for shared_variables 
    private static final String KEY_num = "num";
    private static final String KEY_Cross = "Cross";
    private static final String KEY__S2 = "_S2";
    private static final String KEY__S3 = "_S3";

    public static class conductMaster extends Master {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public conductMaster (CommandLine line) {
            // parse command-line arguments (if any)
            java.util.HashMap<String,String> arg_map = new java.util.HashMap<String,String>();
            gps.node.Utils.parseOtherOptions(line, arg_map);

            if (arg_map.containsKey("num")) {
                String s = arg_map.get("num");
                num = Integer.parseInt(s);
            }
        }

        //save output
        public void writeOutput(BufferedWriter bw) throws IOException {
            bw.write("num:\t" + num + "\n");
            bw.write("Cross:\t" + Cross + "\n");
            bw.write("_S2:\t" + _S2 + "\n");
            bw.write("_S3:\t" + _S3 + "\n");
        	bw.write("_ret_value:\t" + _ret_value + "\n");
            
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private int num;
        private int Cross;
        private int _S2;
        private int _S3;
        private float _ret_value; // the final return value of the procedure

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
            _S2 = 0;
            _S3 = 0;
            Cross = 0;
            -----*/
            System.out.println("Running _master_state 0");

            _S2 = 0 ;
            _S3 = 0 ;
            Cross = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (u : G.Nodes)
            {
                If ((u.member == num) )
                    _S2 += u.Degree() @ u ;
                If ((u.member != num) )
                    _S3 += u.Degree() @ u ;
                If (u.member == num)
                {
                    Foreach (j : u.Nbrs)
                    {
                        If (j.member != num)
                        {
                            Cross += 1 @ u ;
                        }
                    }
                }
            }
            -----*/
            System.out.println("Running _master_state 2");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_num,new IntOverwriteGlobalObject(num));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_Cross,new IntSumGlobalObject(0));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__S2,new IntSumGlobalObject(0));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__S3,new IntSumGlobalObject(0));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 4;
            _master_should_start_workers = true;
        }
        private void _master_state_4() {
            /*------
            -----*/
            System.out.println("Running _master_state 4");
            Cross = Cross+((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY_Cross)).getValue().getValue();
            _S2 = _S2+((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY__S2)).getValue().getValue();
            _S3 = _S3+((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY__S3)).getValue().getValue();

            _master_state_nxt = 5;
        }
        private void _master_state_5() {
            /*------
            //Receive Nested Loop
            Foreach (j : u.Nbrs)
            {
                If (j.member != num)
                {
                    Cross += 1 @ u ;
                }
            }
            -----*/
            System.out.println("Running _master_state 5");
            getGlobalObjectsMap().clearNonDefaultObjects();
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_num,new IntOverwriteGlobalObject(num));
            getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_Cross,new IntSumGlobalObject(0));
            getGlobalObjectsMap().putOrUpdateGlobalObject("__gm_gps_state",new IntOverwriteGlobalObject(_master_state));

            _master_state_nxt = 3;
            _master_should_start_workers = true;
        }
        private void _master_state_3() {
            /*------
            Din = _S2;
            Dout = _S3;
            m =  (Float ) ((Din < Dout)  ? Din : Dout);
            If (m == 0)
                Return (Cross == 0)  ? 0.000000 : +INF;
            Else
                Return  (Float ) Cross / m;
            -----*/
            System.out.println("Running _master_state 3");
            Cross = Cross+((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(KEY_Cross)).getValue().getValue();
            int Din;
            int Dout;
            float m;

            Din = _S2 ;
            Dout = _S3 ;
            m = (float)((Din < Dout)?Din:Dout) ;
            if (m == 0)
                _ret_value = (Cross == 0)?(float)(0.000000):Float.MAX_VALUE;
            else
                _ret_value = ((float)Cross) / m;

            _master_should_finish = true;
        }
    }

    //----------------------------------------------
    // Main Vertex Class
    //----------------------------------------------
    public static class conductVertex
        extends NullEdgeVertex< conduct.VertexData, conduct.MessageData > {

        public conductVertex(CommandLine line) {
            // todo: how to tell if we should parse the command lines or not
            // --> no need. master will parse the command line and sent it to the workers
        }

        @Override
        public VertexData getInitialValue(int id) {
            return new VertexData(id % 2);//getRandom().nextInt(2));
        }

        @Override
        public void compute(Iterable<conduct.MessageData> _msgs, int _superStepNo) {
            // todo: is there any way to get this value quickly?
            // (this can be done by the first node and saved into a static field)
            int _state_vertex = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("__gm_gps_state")).getValue().getValue();
            switch(_state_vertex) { 
                case 2: _vertex_state_2(_msgs); break;
                case 5: _vertex_state_5(_msgs); break;
            }
        }
        private void _vertex_state_2(Iterable<conduct.MessageData> _msgs) {
            VertexData _this = getValue();
            int num = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_num)).getValue().getValue();
            /*------
            Foreach (u : G.Nodes)
            {
                If ((u.member == num) )
                    _S2 += u.Degree() @ u ;
                If ((u.member != num) )
                    _S3 += u.Degree() @ u ;
                If (u.member == num)
                {
                    Foreach (j : u.Nbrs)
                    {
                        If (j.member != num)
                        {
                            Cross += 1 @ u ;
                        }
                    }
                }
            }
            -----*/

            {
                if ((_this.member == num))
                    getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__S2,new IntSumGlobalObject(getNeighborsSize()));
                if ((_this.member != num))
                    getGlobalObjectsMap().putOrUpdateGlobalObject(KEY__S3,new IntSumGlobalObject(getNeighborsSize()));
                if (_this.member == num)
                {
                    // Sending messages to all neighbors
                    MessageData _msg = new MessageData((byte) 0);
                    sendMessages(getNeighborIds(), _msg);
                }
            }
        }
        private void _vertex_state_5(Iterable<conduct.MessageData> _msgs) {
            VertexData _this = getValue();
            int num = ((IntOverwriteGlobalObject)getGlobalObjectsMap().getGlobalObject(KEY_num)).getValue().getValue();

            // Begin msg receive
            for(MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                Foreach (j : u.Nbrs)
                {
                    If (j.member != num)
                    {
                        Cross += 1 @ u ;
                    }
                }
                -----*/
                if (_this.member != num)
                {
                    getGlobalObjectsMap().putOrUpdateGlobalObject(KEY_Cross,new IntSumGlobalObject(1));
                }
            }

        }
    } // end of Vertex

    //----------------------------------------------
    // Factory Class
    //----------------------------------------------
    public static class conductVertexFactory
        extends NullEdgeVertexFactory< conduct.VertexData, conduct.MessageData > {
        @Override
        public NullEdgeVertex< conduct.VertexData, conduct.MessageData > newInstance(CommandLine line) {
            return new conductVertex(line);
        }
    } // end of VertexFactory

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData extends MinaWritable {
        // properties
        int member;

        public VertexData(int member) {
        	this.member = member;
        }
		@Override
        public int numBytes() {return 4;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.putInt(member);
        }
        @Override
        public void read(IoBuffer IOB) {
            member= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            member= Utils.byteArrayToIntBigEndian(_BA, _idx + 0);
            return 4;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 4);
            return 4;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "";
        }
        //Input Data Parsing
        @Override
        public void read(String inputString) {
            this.member = Integer.parseInt(inputString);
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


    // job description for the system
    public static class JobConfiguration extends GPSJobConfiguration {
        @Override
        public Class<?> getMasterClass() {
            return conductMaster.class;
        }
        @Override
        public Class<?> getVertexClass() {
            return conductVertex.class;
        }
        @Override
        public Class<?> getVertexFactoryClass() {
            return conductVertexFactory.class;
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
