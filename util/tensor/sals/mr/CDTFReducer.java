/* =================================================================================
 *
 * Fully Scalable Methods for Distributed Tensor Factorization
 * Authors: Kijung Shin (kijungs@cs.cmu.edu), Lee Sael, U Kang
 *
 * Version: 1.0
 * Date: April 10, 2016
 * Main Contact: Kijung Shin (kijungs@cs.cmu.edu)
 *
 * This software is free of charge under research purposes.
 * For commercial purposes, please contact the author.
 *
 * =================================================================================
 */


package util.tensor.sals.mr;

import static util.tensor.sals.mr.Params.P_AVERAGE;
import static util.tensor.sals.mr.Params.P_SEED;
import static util.tensor.sals.mr.Params.P_USE_BIAS;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import util.tensor.sals.single.ArrayMethods;

/**
 * Reducer for the Hadoop version of CDTF
 * <P>
 * @author Kijung
 */
public class CDTFReducer extends CommonReducer {

    ////////////////////////////////////
    //private fields
    ////////////////////////////////////

    private boolean useBias = false; // whether to use bias terms
    private float[][] oldCols; // values of parameters before the rank one factorization
    private float[][] curCols; // values of parameters after the rank one factorization
    private float mu = 0; //the average of the training entries


    ////////////////////////////////////
    //public methods
    ////////////////////////////////////

    /**
     * load parameters and initialize resources
     * @param context
     */
    @Override
    public void setup(Context context){

        super.setup(context);

        oldCols = new float[N][];
        curCols = new float[N][];

        for(int mode=0; mode<N; mode++){
            oldCols[mode] = new float[modeLengths[mode]];
            curCols[mode] = new float[modeLengths[mode]];
        }

        useBias = conf.getBoolean(P_USE_BIAS, false);
        if(useBias){
            mu = conf.getFloat(P_AVERAGE, 0);
        }
    }

    /**
     * Cache distributed data to local disk
     * @param key <machine to assign this entry, order of the factor matrix that this entry is used to update>
     * @param values list of <indices of the entry, value of the entry>
     * @param context
     * @throws IOException
     * @throws FileNotFoundException
     */
    @Override
    public void reduce(TripleWritable key, Iterable<ElementWritable> values, Context context) throws FileNotFoundException, IOException{

        if(machineId < 0){ //first

            machineId = key.left;
			
			/*
			 * Initialize Local Path
			 */
            String userHome = System.getProperty("user.home");
            baseLocalPath = userHome+"/CDTF_"+machineId;
            tempLocalFile = baseLocalPath+"/TEMP";
            File baseDir = new File(baseLocalPath);

            if (baseDir.exists())
                try {
                    FileUtil.fullyDelete(baseDir);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            baseDir.mkdir();

            for(int dim=0; dim<N; dim++){
                File f = new File(getLocalParamPath(dim));
                if(f.exists())
                    f.delete();
                f.mkdir();
            }

            for(int mode=0; mode<N; mode++){
                outIndexR[mode] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(mode, true, CommonMapper.TYPE_TRAINING, false))));
                outValueR[mode] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(mode, false, CommonMapper.TYPE_TRAINING, false))));
            }
            outIndexRTest[0] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(0, true, CommonMapper.TYPE_TEST, false))));
            outValueRTest[0] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(0, false, CommonMapper.TYPE_TEST, false))));
            outIndexRQuery[0] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(0, true, CommonMapper.TYPE_QUERY, false))));
            outValueRQuery[0] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(0, false, CommonMapper.TYPE_QUERY, false))));

            for(int mode=0; mode<N; mode++){
                startIndex[mode] = getStartIndex(mode, machineId);
                endIndex[mode] = getStartIndex(mode, machineId+1);
                if(machineId==M){
                    endIndex[mode] +=1;
                }
                nnzFiber[mode] = new int[endIndex[mode] - startIndex[mode]];
            }
        }


        int fileMode = key.mid;
        for(ElementWritable value : values){
            if(value.isTraining){
                nnzTraining[fileMode]++;
                int[] index = value.index;
                for(int dim = 0; dim <N; dim++){
                    outIndexR[fileMode].writeInt(index[dim]);
                }
                outValueR[fileMode].writeFloat(value.value-mu);
                nnzFiber[fileMode][index[fileMode]-startIndex[fileMode]]++;
            }
            else {

                if(Float.isNaN(value.value)){ //query
                    nnzQuery[fileMode]++;
                    int[] index = value.index;
                    for(int dim = 0; dim <N; dim++){
                        outIndexRQuery[fileMode].writeInt(index[dim]);
                    }
                    outValueRQuery[fileMode].writeFloat(-mu);
                }
                else {
                    nnzTest[fileMode]++;
                    int[] index = value.index;
                    for(int dim = 0; dim <N; dim++){
                        outIndexRTest[fileMode].writeInt(index[dim]);
                    }
                    outValueRTest[fileMode].writeFloat(value.value-mu);
                }
            }
        }

    }



    /**
     * CDTF main procedure
     * @param context
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{


        for(int i=0; i<outIndexR.length; i++){
            outIndexR[i].close();
            outValueR[i].close();
        }
        outIndexRTest[0].close();
        outValueRTest[0].close();
        outIndexRQuery[0].close();
        outValueRQuery[0].close();

		
		/*
		 * initialize parameters
		 */
        Random rand = new Random(conf.getInt(P_SEED, 0));
        for(int n=0; n<N; n++){
            context.progress();
            createParamMatrix(n, K, n==0 ? 0 : 1, rand, context);
        }

        if(useBias){
            for(int n=0; n<N; n++){
                context.progress();
                createBiasTerms(n, context);
            }
        }

        double[][] result = new double[Tout][6]; //performance statistics

        long startTime = System.currentTimeMillis();

        for(int outIter = 0; outIter<Tout; outIter++){

            FileSystem fs = FileSystem.get(conf);

            for(int k=0; k<K; k++){

                System.out.printf("Iter : %d, column : %d\n", outIter, k);

                //load the kth column of the factor matrices
                for(int n=0; n<N; n++){
                    long time = System.currentTimeMillis();
                    context.progress();
                    loadFromLocal(n, k);
                    context.getCounter("Speed", "Initialize").increment(System.currentTimeMillis()-time);
                }

                oldCols = ArrayMethods.copy(curCols);

                for(int innerIter = 0; innerIter<Tin; innerIter++){

					/*
					 *  rank one factorization
					 */
                    for(int n=0; n<N; n++){

                        context.progress();
                        long time = System.currentTimeMillis();

                        // update a^{(n)}_{*k}: the kth column of the A^{(n)}
                        updateFactors(n);
                        context.getCounter("Speed", "Optimize").increment(System.currentTimeMillis()-time);

                        time = System.currentTimeMillis();

                        // broadcast updated parameters
                        communicate(outIter, k, innerIter, n, context, fs);
                        context.getCounter("Speed", "Broadcast").increment(System.currentTimeMillis()-time);
                    }
                }

                double trainErrorSum = 0;
                double testErrorSum = 0;
                int NNZSum = 0;
                int NNZTestSum = 0;

				/*
				 * update R
				 */
                for(int n=0; n<N; n++){
                    context.progress();

                    long time = System.currentTimeMillis();

                    if(n==0){
                        trainErrorSum += updateR(n, CommonMapper.TYPE_TRAINING, !useBias&&k==K-1);
                        testErrorSum += updateR(n, CommonMapper.TYPE_TEST, !useBias&&k==K-1);
                        updateR(n, CommonMapper.TYPE_QUERY, false);
                        NNZSum += nnzTraining[n];
                        NNZTestSum += nnzTest[n];
                    }
                    else {
                        updateR(n, CommonMapper.TYPE_TRAINING, false);
                    }
                    context.getCounter("Speed", "Update R").increment(System.currentTimeMillis()-time);
                    time = System.currentTimeMillis();
                    writeFactors(n, k);
                    context.getCounter("Speed", "Update Param").increment(System.currentTimeMillis()-time);
                }

                if(!useBias&&k==K-1){
                    System.out.println(Math.sqrt(trainErrorSum/NNZSum));
                    System.out.println(Math.sqrt(testErrorSum/NNZTestSum));
                    result[outIter] = new double[]{outIter, System.currentTimeMillis()-startTime, trainErrorSum, NNZSum, testErrorSum, NNZTestSum};

                }
            }

            // update bias terms
            if(useBias){

                for(int n=0; n<N; n++){

                    long time = System.currentTimeMillis();
                    context.progress();
                    loadBiasFromLocal(n);
                    context.getCounter("Speed", "Initialize").increment(System.currentTimeMillis()-time);

                    time = System.currentTimeMillis();
                    oldBias = curBias.clone();

                    context.progress();

                    // update b^{(n)}
                    updateBias(n);
                    context.getCounter("Speed", "Optimize").increment(System.currentTimeMillis()-time);

                    time = System.currentTimeMillis();

                    // broadcast updated parameters
                    communicateBias(outIter, n, context, fs);
                    context.getCounter("Speed", "Broadcast").increment(System.currentTimeMillis()-time);


                    float trainErrorSum = 0;
                    float testErrorSum = 0;
                    int NNZSum = 0;
                    int NNZTestSum = 0;
					
					/*
					 * update R
					 */
                    for(int nr=0; nr<N; nr++){
                        context.progress();

                        time = System.currentTimeMillis();

                        if(nr==0){
                            trainErrorSum += updateRWithBias(nr, n, CommonMapper.TYPE_TRAINING, n==N-1);
                            testErrorSum += updateRWithBias(nr, n, CommonMapper.TYPE_TEST, n==N-1);
                            updateRWithBias(nr, n, CommonMapper.TYPE_QUERY, false);
                            NNZSum += nnzTraining[nr];
                            NNZTestSum += nnzTest[nr];
                        }
                        else {
                            updateRWithBias(nr, n, CommonMapper.TYPE_TRAINING, false);
                        }
                        context.getCounter("Speed", "Update R").increment(System.currentTimeMillis()-time);
                        time = System.currentTimeMillis();
                    }

                    writeBiasParams(n);
                    context.getCounter("Speed", "Update Param").increment(System.currentTimeMillis()-time);

                    if(n==N-1){
                        System.out.println(Math.sqrt(trainErrorSum/NNZSum));
                        System.out.println(Math.sqrt(testErrorSum/NNZTestSum));
                        result[outIter] = new double[]{outIter, System.currentTimeMillis()-startTime, trainErrorSum, NNZSum, testErrorSum, NNZTestSum};

                    }
                }
            }

            if(machineId==0){
                context.getCounter("Time ", ""+outIter).increment(System.currentTimeMillis()-startTime);
            }
            try { fs.close();} catch(Exception e){};

        }

        nnzFiber = null;
        curCols = null;
        oldCols = null;

        //write estimate
        if(nnzQuery[0]>0){
            writeEstimate(context);
        }

        //write performance statistics
        writePerformance(context, result);

        //write factor matrices and biases
        if(machineId==0){
            writeFactormatricesResult(context);
            if(useBias)
                writeBiasesResults(context);
        }

        //delete temporary files
        File file = new File(baseLocalPath);
        if(file.exists()){
            try {
                FileUtil.fullyDelete(file);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    ////////////////////////////////////
    //private methods
    ////////////////////////////////////

    /**
     * Update factor matrices
     * update a^{(n)}_{*k}: the kth column of the A^{(n)}
     * @param n	mode
     * @throws IOException
     * @throws FileNotFoundException
     */
    private void updateFactors(int n) throws FileNotFoundException, IOException {

        int blockLength = endIndex[n] - startIndex[n];
        float[] numerators = new float[blockLength];
        float[] denominators = new float[blockLength];

        ObjectInputStream inIndex = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, true, CommonMapper.TYPE_TRAINING, false))));
        ObjectInputStream inValue = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, false, CommonMapper.TYPE_TRAINING, false))));

        for(int elem=0; elem<nnzTraining[n]; elem++){

            int[] index = new int[N];
            float r = 0;

            for(int _mode=0; _mode<N; _mode++){
                index[_mode] = inIndex.readInt();
            }
            r = inValue.readFloat();

            float oldProduct = 1;
            float numerator = 1;
            float denominator = 1;
            for(int dim=0; dim<N; dim++){
                oldProduct *= oldCols[dim][index[dim]];
                if(dim!=n){
                    numerator *= curCols[dim][index[dim]];
                }
            }
            denominator = numerator*numerator;
            numerator *= r + oldProduct;
            int resultIndex = index[n]-startIndex[n];
            numerators[resultIndex] += numerator;
            denominators[resultIndex] += denominator;
        }

        inIndex.close();
        inValue.close();


        if(regularization == 1) {

            for (int i = 0; i < blockLength; i++) {
                if (denominators[i] != 0) {
                    float g = -2 * numerators[i];
                    float d = 2 * denominators[i];
                    float result = 0;
                    float weightedLambda = lambda * (useWeight ? nnzFiber[n][i] : 1);
                    if (g > weightedLambda) {
                        result = (weightedLambda - g) / d;
                    } else if (g < -weightedLambda) {
                        result = -(weightedLambda + g) / d;
                    }

                    if (result > -epsilon && result < epsilon) { // to prevent underflow
                        result = 0;
                    }

                    if (nonNegative) {
                        result = Math.max(result, 0);
                    }

                    int rowIndex = i + startIndex[n];
                    curCols[n][rowIndex] = result;
                }
            }
        } else if(regularization == 2) {

            for(int i=0; i<blockLength; i++){
                if(denominators[i]!=0){
                    denominators[i] += lambda * (useWeight ? nnzFiber[n][i] : 1);
                    int rowIndex = i+startIndex[n];
                    float result = numerators[i] / denominators[i];
                    if(result > -epsilon && result < epsilon){ // to prevent underflow
                        result = 0;
                    }
                    if (nonNegative) {
                        result = Math.max(result, 0);
                    }
                    curCols[n][rowIndex] = result;
                }
            }
        }
    }

    /**
     * update R entries {}_{m}R^{(n)}
     * @param n	mode
     * @param type training / test / query
     * @param measureCost whether to calculate error
     * @return	sum of errors
     * @throws IOException
     * @throws FileNotFoundException
     */
    private double updateR(int n, int type, final boolean measureCost) throws FileNotFoundException, IOException{

        double errorSum = 0;

        ObjectInputStream inIndex = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, true, type, false))));
        ObjectInputStream inValue = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, false, type, false))));
        ObjectOutputStream outValue = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(getLocalRPath(n, false, type, true))));

        int nnz = 0;
        if(type==CommonMapper.TYPE_TRAINING)
            nnz = nnzTraining[n];
        else if(type==CommonMapper.TYPE_TEST)
            nnz = nnzTest[n];
        else
            nnz = nnzQuery[n];

        for(int elem=0; elem<nnz; elem++){
            int[] index = new int[N];
            float r = 0;
            for(int _mode=0; _mode<N; _mode++){
                index[_mode] = inIndex.readInt();
            }
            r = inValue.readFloat();

            float oldProduct = 1;
            float newProduct = 1;
            for(int _mode=0; _mode<N; _mode++){
                oldProduct *= oldCols[_mode][index[_mode]];
                newProduct *= curCols[_mode][index[_mode]];
            }
            r = (r+oldProduct) - newProduct;
            outValue.writeFloat(r);


            if(measureCost)
                errorSum += r*r;
        }


        inIndex.close();
        inValue.close();
        outValue.close();


        replace(getLocalRPath(n, false, type, false));

        return errorSum;
    }

    /**
     * send updated parameters to other machines and receive updated parameters from other machines.
     * broadcast {}_{m}a^{(n)}_{*k} and receive a^{(n)}_{*k}
     * @param outIter
     * @param k column
     * @param inIter
     * @param n	mode
     * @param context
     * @param fs
     * @throws IOException
     */
    private void communicate(int outIter, int k, int inIter, int n, Context context, FileSystem fs) throws IOException{

        FSDataOutputStream out = null;

        //write mine {}_{m}a^{(n)}_{*k}
        Path outPath = new Path(getHDFSParamPath(outIter, k, inIter, n, machineId, false));
        out = fs.create(outPath);
        for(int i=startIndex[n]; i<endIndex[n]; i++){
            out.writeFloat(curCols[n][i]);
        }
        out.close();

        markWrite(outIter, k, inIter, n, fs);

        //check if others write their file and read it
        boolean[] markReadComplete = new boolean[M];
        markReadComplete[machineId] = true;
        while(true){

            long requestTime = System.currentTimeMillis();
            FileStatus[] statusList = fs.listStatus(new Path(getHDFSParamPath(outIter, k, inIter, n, true)));
            shuffle(statusList);

            for(FileStatus status : statusList){

                int _machineId = Integer.valueOf(status.getPath().getName());

                if(markReadComplete[_machineId])
                    continue;
                else {
                    FSDataInputStream in = null;
                    try{
                        in = fs.open(new Path(getHDFSParamPath(outIter, k, inIter, n, _machineId, false)));
                        for(int i=getStartIndex(n, _machineId); i<getStartIndex(n, _machineId+1); i++){
                            curCols[n][i]=in.readFloat();
                        }
                    } catch(Exception e){
                        System.out.println(e.getMessage());
                        context.getCounter("Error", "err").increment(1);
                        continue;
                    } finally{
                        try { in.close(); } catch (Exception e) {}
                    }
                    markReadComplete[_machineId]=true;
                }
            }

            boolean markAll = true;
            for(int _machineId=0; _machineId<M; _machineId++){
                if(!markReadComplete[_machineId]){
                    markAll = false;
                    break;
                }
            }

            if(markAll){
                break;
            }
            else {
                context.progress();
                long timeToWait = (long)(waiting * Math.random()) - (System.currentTimeMillis() - requestTime); //avoid simultaneous request
                if(timeToWait > 0){
                    try {
                        Thread.sleep(timeToWait);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }
        }
    }

    /**
     * write the updated parameters
     * write a^{(n)}_{*k}: the kth column of the A^{(n)}
     * @param n	mode
     * @param k	column
     * @throws IOException
     * @throws FileNotFoundException
     */
    private void writeFactors(int n, int k) throws FileNotFoundException, IOException{
        ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalParamPath(n, k, false))));
        for(int row=0; row<modeLengths[n]; row++){
            os.writeFloat(curCols[n][row]);
        }
        os.close();
    }

    /**
     * load parameters a^{(n)}_{*k} from local disk
     * @param n	mode
     * @param k	column
     * @throws IOException
     * @throws FileNotFoundException
     */
    private void loadFromLocal(int n, int k) throws FileNotFoundException, IOException{
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream (new FileInputStream(getLocalParamPath(n, k, false))));
        for(int i=0; i<modeLengths[n]; i++){
            curCols[n][i]=in.readFloat();
        }

        in.close();
    }
}