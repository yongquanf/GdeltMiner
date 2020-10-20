package EventGraph.graphStream.RepresentationLearning.tsne;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import javax.swing.JFrame;

import org.math.io.files.BinaryFile;
import org.math.plot.FrameView;
import org.math.plot.Plot2DPanel;
import org.math.plot.PlotPanel;
import org.math.plot.plots.ScatterPlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * tsne, Hinton et al.
 * @author eric
 *
 */
public class TSneBinaryDemo {

	private static final Logger logger = LoggerFactory.getLogger(TSneBinaryDemo.class);
	static double perplexity = 5.0;
	private static int initial_dims = 50;

	static TSne tsne = null;
	static TSneConfiguration config =null;
	
	
	public TSneBinaryDemo() {}

	/**
	 * tsne
	 * @param matrix
	 * @return 2d matrix
	 */
	public static double[][] runTSne(double [][] matrix) {
		
		//instance
		 tsne = new SimpleTSne();
		logger.info("Shape is: " + matrix.length + " x " + matrix[0].length);
		//config
		config = TSneUtils.buildConfig(matrix, 2, initial_dims, perplexity, 1000);
		double [][] Y = tsne.tsne(config);
		logger.info("Result is = " + Y.length + " x " + Y[0].length + " => \n" + MatrixOps.doubleArrayToPrintString(Y));
		displayResult(Y);
		return Y;
	}

	static void displayResult(double[][] Y) {
		Plot2DPanel plot = new Plot2DPanel();

		ScatterPlot dataPlot = new ScatterPlot("Data", PlotPanel.COLORLIST[0], Y);
		plot.plotCanvas.setNotable(true);
		plot.plotCanvas.setNoteCoords(true);
		plot.plotCanvas.addPlot(dataPlot);

		FrameView plotframe = new FrameView(plot);
		plotframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		plotframe.setVisible(true);
	}

	static double[][] doubleArrayToMatrix(double[] d, int rows, int cols) {
		double [][] matrix = new double[rows][cols];
		int idx = 0;
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				matrix[i][j] = d[idx++];
			}
		}
		return matrix;
	}

	static double[][] floatArrayToMatrix(float[] d, int rows, int cols) {
		double [][] matrix = new double[rows][cols];
		int idx = 0;
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				matrix[i][j] = d[idx++];
			}
		}
		return matrix;
	}

	static double[][] intArrayToMatrix(int[] d, int rows, int cols) {
		double [][] matrix = new double[rows][cols];
		int idx = 0;
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				matrix[i][j] = d[idx++];
			}
		}
		return matrix;
	}

	public static double [][] loadData(String [] args) {
		String usage = "Usage: TSneBinaryDemo [options] -asMatrix ROWS COLS file \nAvailable options:\n  -endian <big|little, default = big>\n  -data <double|float|int, default = double>" 
				     + "Example: TSneBinaryDemo -data double -asMatrix 1000 20 Theta_DxK_1000_20_05000.BINARY";
		
		File file = null;
		String dataType = "double";
		String endianness = BinaryFile.BIG_ENDIAN;
		int rows = 0;
		int cols = 0;
		int printSampleSize = 10;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-endian")) {
				if (args[i + 1].equals("little"))
					endianness = BinaryFile.LITTLE_ENDIAN;
				i++;
			} else if (args[i].equals("-data")) {
				dataType = args[i + 1];
				i++;
			} else if (args[i].equals("-asMatrix")) {
				String rowS = args[i + 1];
				rows = Integer.parseInt(rowS);
				i++;
				String colS = args[i + 1];
				cols = Integer.parseInt(colS);
				i++;
			} else {
				file = new File(args[i]);
				if (!file.exists()) {
					logger.info("File " + file + " doesn't exists.\n" + usage);
					return null;
				}
				i++;
			}
		}

		if (dataType.equals("double")) {
			double[] d = BinaryFile.readDoubleArray(file, endianness);
			double [][] matrix = doubleArrayToMatrix(d,rows,cols);
			printDataSample(printSampleSize, matrix);              	
			return matrix;
		} else if (dataType.equals("float")) {
			float[] d = BinaryFile.readFloatArray(file, endianness);
			double [][] matrix = floatArrayToMatrix(d,rows,cols);
			printDataSample(printSampleSize, matrix);              	
			return matrix;				
		} else if (dataType.equals("int")) {
			int[] d = BinaryFile.readIntArray(file, endianness);
			double [][] matrix = intArrayToMatrix(d,rows,cols);
			printDataSample(printSampleSize, matrix);
			return matrix;	
		} else {
			logger.info(usage);
		}
		return null;
	}

	/**
	 * process to double file
	 * @param args
	 * @return
	 * @throws Exception 
	 */
	public static double [][] loadDataTensor(String [] args) throws Exception {
		String usage = "Usage: TSneBinaryDemo [options] -asMatrix ROWS COLS file \nAvailable options:\n  -endian <big|little, default = big>\n  -data <double|float|int, default = double>" 
				     + "Example: TSneBinaryDemo -data double -asMatrix 1000 20 factorFile";
		
		String factor=null;
		File FactorFile = null;
		String dataType = "double";
		String endianness = BinaryFile.BIG_ENDIAN;
		int rows = 0;
		int cols = 0;
		int printSampleSize = 10;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-endian")) {
				if (args[i + 1].equals("little"))
					endianness = BinaryFile.LITTLE_ENDIAN;
				i++;
			} else if (args[i].equals("-data")) {
				dataType = args[i + 1];
				i++;
			} else if (args[i].equals("-asMatrix")) {
				String rowS = args[i + 1];
				rows = Integer.parseInt(rowS);
				i++;
				String colS = args[i + 1];
				cols = Integer.parseInt(colS);
				i++;
			} else {
				factor = args[i];
				FactorFile = new File(factor);
				if (!FactorFile.exists()) {
					logger.info("File " + FactorFile + " doesn't exists.\n" + usage);
					return null;
				}
				i++;
			}
		}
		
		String binFile = write2BinaryFile(factor,true,",","DoubleBinary");
		FactorFile = new File(binFile);

		if (dataType.equals("double")) {
			double[] d = BinaryFile.readDoubleArray(FactorFile, endianness);
			double [][] matrix = doubleArrayToMatrix(d,rows,cols);
			printDataSample(printSampleSize, matrix);              	
			return matrix;
		} else if (dataType.equals("float")) {
			float[] d = BinaryFile.readFloatArray(FactorFile, endianness);
			double [][] matrix = floatArrayToMatrix(d,rows,cols);
			printDataSample(printSampleSize, matrix);              	
			return matrix;				
		} else if (dataType.equals("int")) {
			int[] d = BinaryFile.readIntArray(FactorFile, endianness);
			double [][] matrix = intArrayToMatrix(d,rows,cols);
			printDataSample(printSampleSize, matrix);
			return matrix;	
		} else {
			logger.info(usage);
		}
		return null;
	}
	
	
	static void printDataSample(int printSampleSize, double[][] matrix) {
		if(matrix!=null) {
			logger.info("Loaded ("+printSampleSize+" samples): ");
			for (int i = 0; i < Math.min(printSampleSize, matrix.length); i++) {
				for (int j = 0; j < matrix[0].length; j++) {
					System.out.print(matrix[i][j] + ",");
				}
				
			}
			logger.info("...");
		}
	}

	public static void main(String[] args) {
		
		double [][] matrix = loadData(args);
		if(matrix!=null) runTSne(matrix);
	}
	
	/**
	 * export
	 * @param corenesses
	 * @param path
	 * @param delim
	 * @throws IOException
	 */
	public static void export(double[][] corenesses, String path, String delim) throws IOException {

        System.err.println("Exporting tsne result... "+ path);
        BufferedWriter bw = new BufferedWriter(new FileWriter(path));
        for(int i=0; i<corenesses.length; i++) {
        	double[] coord = corenesses[i];
            bw.write(i + delim + Arrays.toString(coord));
            bw.newLine();
        }
        bw.close();
        System.err.println("Result was exported.: "+ path);

    }
	
	
	public static void ProcessFactorMatrix(String TensorFactorDir) {
		try {
			//readFactorMatrixAndWriteBinary();
			
			readFactorMatrixAndWriteBinaryDir(TensorFactorDir);
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * tensor factor
	 * @param TensorFactorDir
	 * @return 
	 * @throws Exception
	 */
	public static String readFactorMatrixAndWriteBinaryDir(String TensorFactorDir) throws Exception {
		
		return write2BinaryFile(TensorFactorDir,true,",","DoubleBinary");
				
	}
	
	
	public static void readFactorMatrixAndWriteBinary() throws Exception {
		int factor1Num = 651;
		int dimension = 8;
		String fileDir = "fileTestTensorFact/factor_matrices/1";
		
		write2BinaryFile(fileDir,true,",","DoubleBinary");
		
		int factor2Num = 648;
		int dimension2 = 8;
		String fileDir2 = "fileTestTensorFact/factor_matrices/2";
		
		write2BinaryFile(fileDir2,true,",","DoubleBinary");
		
		int factor3Num = 91;
		int dimension3 = 8;
		String fileDir3 = "fileTestTensorFact/factor_matrices/3";
		write2BinaryFile(fileDir3,true,",","DoubleBinary");
		
		int factor4Num = 14;
		int dimension4 = 8;
		String fileDir4 = "fileTestTensorFact/factor_matrices/4";
		write2BinaryFile(fileDir4,true,",","DoubleBinary");
	}

	/**
	 * each row:
	 * first: id, rest: separated by deliminator ","
	 * @param raw
	 * @param postfix
	 * @throws Exception 
	 */
	public static String write2BinaryFile(String path,boolean verbose,
			String delim,String postfix) throws Exception {
		// TODO Auto-generated method stub
		 String outFile = path+postfix;
		//write to binary file
		BinaryFile outer = new BinaryFile(new File(outFile),BinaryFile.BIG_ENDIAN);
		
		final BufferedReader br = new BufferedReader(new FileReader(path));
		 while(true) {
	            final String line = br.readLine();
	            if(line == null) {
	                break;
	            }
	            else if(line.startsWith("#") || line.startsWith("%") || line.startsWith("//")) { //comment

	                if(verbose) {
	                    System.err.println("The following line was ignored during loading a graph:");
	                    System.err.println(line);
	                }
	                continue;
	            }
	            else {
	                String[] tokens = line.split(delim);
	                double[] dat = new double[tokens.length-1];
	                //write to binary file
	                for(int index=1;index<tokens.length;index++) {
	                	dat[index-1]=Double.parseDouble(tokens[index]);
	                }
	                outer.writeDoubleArray(dat, true);	
	                }
		 }
		 		
		 return outFile;
	}

	/**
	 * write coord
	 * @param matrix
	 * @throws IOException 
	 */
	public static void writeResult(double[][] matrix, String path) throws IOException {
		// TODO Auto-generated method stub
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(path));
		bw.write(TSneUtils.toString(config));
        for(int i=0; i<matrix.length; i++) {
        	
            bw.write(i + "," + Arrays.toString(matrix[i]));
            bw.newLine();
        }
        bw.close();
	}
	
}
