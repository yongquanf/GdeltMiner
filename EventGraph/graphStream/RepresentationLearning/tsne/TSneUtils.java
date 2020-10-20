package EventGraph.graphStream.RepresentationLearning.tsne;


public class TSneUtils {
	
	public static TSneConfiguration buildConfig(double[][] xin, int outputDims, int initial_dims,
			double perplexity, int max_iter, boolean use_pca, double theta, boolean silent, boolean printError) {
		return new TSneConfig(xin, outputDims, initial_dims, perplexity, max_iter, use_pca, theta, silent, printError);
	}
	
	public static TSneConfiguration buildConfig(double[][] xin, int outputDims, int initial_dims,
			double perplexity, int max_iter, boolean use_pca, double theta, boolean silent) {
		return new TSneConfig(xin, outputDims, initial_dims, perplexity, max_iter, use_pca, theta, silent, true);
	}
	
	public static TSneConfiguration buildConfig(double[][] xin, int outputDims, int initial_dims,
			double perplexity, int max_iter) {
		return new TSneConfig(xin, outputDims, initial_dims, perplexity, max_iter, true, 0.5, false, true);
	}
	
	public static String toString(TSneConfiguration config) {
		return config.getInitialDims()+" "+config.getOutputDims()+" "+config.getMaxIter();
	}

}
