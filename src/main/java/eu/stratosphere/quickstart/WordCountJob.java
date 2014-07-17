package eu.stratosphere.quickstart;

import java.util.Properties;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Stratosphere program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions. 
 * </ul>
 * 
 */
@SuppressWarnings("serial")
public class WordCountJob {
	
	//
	//	Program. 
	//
	
	public static void main(String[] args) throws Exception {
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// get input data
		DataSet<String> text = env.fromElements(
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,"
				);
		
		DataSet<Tuple2<String, Integer>> counts = 
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.aggregate(Aggregations.SUM, 1);

		// emit result
		counts.print();
		
		// execute program
		env.execute("WordCount Example");
		
		/***********************************
		 * Cascade program
		 ***********************************/
		String inPath = args[ 0 ];
		String outPath = args[ 1 ];

	    Properties properties = new Properties();
	    AppProps.setApplicationJarClass( properties, WordCountJob.class );
	    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

	    // create the source tap
	    Tap inTap = new Hfs( new TextDelimited( true, "\t" ), inPath );

	    // create the sink tap
	    Tap outTap = new Hfs( new TextDelimited( true, "\t" ), outPath );

	    // specify a pipe to connect the taps
	    Pipe copyPipe = new Pipe( "copy" );

	    // connect the taps, pipes, etc., into a flow
	    FlowDef flowDef = FlowDef.flowDef()
	     .addSource( copyPipe, inTap )
	     .addTailSink( copyPipe, outTap );

	    // run the flow
	    flowConnector.connect( flowDef ).complete();

	}
	
	//
	// 	User Functions
	//
	
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into 
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter extends FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");
			
			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}