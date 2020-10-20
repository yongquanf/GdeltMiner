package EventGraph.graphStream.RepresentationLearning.HSH;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Matrix_4_HSH {
	
	private static final Logger logger = LoggerFactory.getLogger(Matrix_4_HSH.class);
	public int mat_row;
	public int mat_column;
	public double[][] mat;
	// S r*r matrix
	public Matrix_4_HSH S;
	// H n*r landmark coordinate vector matrix
	public Matrix_4_HSH H;
	// D n*n distance matirx of n nodes
	public Matrix_4_HSH D;

	//
	public Matrix_4_HSH() {
		// row=1;
		// column=1;
		// mat[0][0]=0;
	}

	//
	public Matrix_4_HSH(int m, int n) {
		mat_row = m;
		mat_column = n;
		mat = new double[mat_row][mat_column];
		for (int i = 0; i < mat_row; i++)
			for (int j = 0; j < mat_column; j++)
				mat[i][j] = 0;
	}

	//
	public void Rand() {
		for (int i = 0; i < mat_row; i++)
			for (int j = 0; j < mat_column; j++)
				mat[i][j] = Math.random() * 100;// //////////////
	}

	/**
	 * transform
	 * @return
	 */
	public Matrix_4_HSH ToTranspose() {
		Matrix_4_HSH result = new Matrix_4_HSH(mat_column, mat_row);
		for (int i = 0; i < mat_column; i++)
			for (int j = 0; j < mat_row; j++)
				result.mat[i][j] = mat[j][i];
		return result;
	}

	//
	void CreatSymMatrix() {
		if (mat_row != mat_column)
			return;
		for (int i = 0; i < mat_row; i++) {
			mat[i][i] = 0;
			for (int j = 0; j < i; j++) {
				mat[i][j] = Math.random() * 100;// ////////////////
				mat[j][i] = mat[i][j];
			}
		}
		return;
	}

	// copy another matrix
	public void CopyMatrix(double[][] DistMatrix) {
		if(DistMatrix==null){
			System.err.println("empty distance matrix");
			return;
		}
	
		for (int i = 0; i < mat_row; i++) {
			for (int j = 0; j <mat_column ; j++) {
				mat[i][j] = DistMatrix[i][j];// ////////////////				
			}
		}
		
	}

	//
	public Matrix_4_HSH MatrixMultiply(Matrix_4_HSH temp) {
		
		if (temp==null) {
			System.err.println("column "+mat_column+" row "+ mat_row);
			return null;
		} else {
			
			Matrix_4_HSH result = new Matrix_4_HSH(mat_row, temp.mat_column);
			for (int i = 0; i < mat_row; i++)
				for (int j = 0; j < temp.mat_column; j++)
					for (int k = 0; k < mat_column; k++)
						result.mat[i][j] += mat[i][k] * temp.mat[k][j];
			return result;
		}
	}

	// }
	public Matrix_4_HSH DotMultiply(Matrix_4_HSH temp) {
		if (temp == null) {
			System.err.println(" null temp in   DotMultiply");
			return null;
		}
	/*	if (mat_column != temp.mat_column || mat_row != temp.mat_row) {
			logger.debug("");
			return null;
		} else {
			*/
			//logger.debug("$: row: "+mat_row+", column"+ mat_column+"\n row: "+temp.mat_row+", column: "+temp.mat_column);
			Matrix_4_HSH result = new Matrix_4_HSH(mat_row, mat_column);
			for (int i = 0; i < mat_row; i++)
			{	for (int j = 0; j < mat_column; j++){
					result.mat[i][j] = mat[i][j] * temp.mat[i][j];
					}
			}
			return result;
		//}
	}

	//
	public Matrix_4_HSH DotDivide(Matrix_4_HSH temp) {
		if (temp == null) {
			System.err.println(" null temp in DotDivide ");
			return null;
		}

		/*if (mat_column != temp.mat_column || mat_row != temp.mat_row) {
			logger.debug("");
			return null;
		} else {*/
			Matrix_4_HSH result = new Matrix_4_HSH(mat_row, mat_column);
			for (int i = 0; i < mat_row; i++) {
				for (int j = 0; j < mat_column; j++) {
					if (Math.abs(temp.mat[i][j]) < 0.00000002) {
						System.err.println("zero in division "+temp.mat[i][j]);
						return null;
					}
					result.mat[i][j] = mat[i][j] / temp.mat[i][j];
				}
			}
			return result;
		//}
	}

	//
	public Matrix_4_HSH DotAdd(Matrix_4_HSH temp) {
		if (mat_column != temp.mat_column || mat_row != temp.mat_row) {
			System.err.println("inconsistent DotAdd");
			return null;
		} else {
			Matrix_4_HSH result = new Matrix_4_HSH(mat_row, mat_column);
			for (int i = 0; i < mat_row; i++)
				for (int j = 0; j < mat_column; j++)
					result.mat[i][j] = mat[i][j] + temp.mat[i][j];
			return result;
		}
	}

	//
	public Matrix_4_HSH Sqrt() {
		Matrix_4_HSH result = new Matrix_4_HSH(mat_row, mat_column);
		for (int i = 0; i < mat_row; i++)
			for (int j = 0; j < mat_column; j++) {
				if (mat[i][j] >= 0){
					result.mat[i][j] = Math.sqrt(mat[i][j]);
				}
				else {
					System.err.println("negative mat[i][j]"+i+", "+j+" @ "+mat[i][j]);
					return null;
				}
			}
		return result;
	}

	// ��������
	// ���÷���A.Inverse();
	public Matrix_4_HSH Inverse() {
		if (mat_row != mat_column) {
			logger.debug("����������󣬾���Ϊ����?�?");
			return null;
		}

		// ����[A|I]
		Matrix_4_HSH temp = new Matrix_4_HSH(mat_row, 2 * mat_column);
		for (int i = 0; i < mat_row; i++)
			for (int j = 0; j < mat_column; j++)
				temp.mat[i][j] = mat[i][j];
		for (int j = 0; j < mat_column; j++)
			temp.mat[j][j + mat_column] = 1;

		for (int i = 0; i < mat_row; i++) {
			if (temp.mat[i][i] != 1) {
				double bs = temp.mat[i][i];
				temp.mat[i][i] = 1;
				for (int p = i + 1; p < temp.mat_column; p++)
					temp.mat[i][p] /= bs;
			}
			for (int q = 0; q < mat_row; q++) {
				if (q != i) {
					double bs = temp.mat[q][i];
					for (int p = 0; p < temp.mat_column; p++)
						temp.mat[q][p] -= bs * temp.mat[i][p];
				} else
					continue;
			}
		}
		Matrix_4_HSH result = new Matrix_4_HSH(mat_row, mat_column);
		for (int i = 0; i < mat_row; i++)
			for (int j = 0; j < mat_column; j++)
				result.mat[i][j] = temp.mat[i][j + mat_column];
		return result;
	}

	// ��ʾ����
	public void Print() {
		if (this == null) {
			logger.debug("null matrix");
			return;
		}
		logger.debug("\n\n==============================\n\n");
		boolean open=true;
		
		
		try {
			for (int i = 0; i < mat_row; i++) {
				for (int j = 0; j < mat_column; j++) {
					
					System.out.print(mat[i][j]+"\t");
					if(open){
						logger.debug(mat[i][j] + "\t");
					}
				}
				
				 if(open){
					 logger.debug("\n");
				 }
			}
			
			logger.debug("\n\n==============================\n\n");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// ��������ľ���H��S������DD=H*S*H_,��ԭʼ��D֮������
	public void CheckError() {
		if (H.mat_row != mat_row || H.mat_column != S.mat_row) {
			logger.debug("���ܼ������������������");
			return;
		}
		// H_Ϊ����H��ת��
		Matrix_4_HSH H_ = new Matrix_4_HSH(H.mat_column, H.mat_row);
		H_ = H.ToTranspose();
		// DD=H*S*H,��D�Ľ���
		Matrix_4_HSH DD = new Matrix_4_HSH(mat_row, mat_column);
		DD = H.MatrixMultiply(S.MatrixMultiply(H_));
		for (int i = 0; i < mat_row; i++)
			for (int j = 0; j < mat_column; j++) {
				DD.mat[i][j] = Math.abs(DD.mat[i][j] - D.mat[i][j]);
			}
		logger.debug("\nABSOLUTE ERROR:");
		try {
			logger.debug("ABSOLUTE ERROR:\n");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DD.Print();
		for (int i = 0; i < mat_row; i++)
			for (int j = 0; j < mat_column; j++) {
				if (i == j) {
					DD.mat[i][j] = -1;
					continue;
				}
				DD.mat[i][j] = DD.mat[i][j] / D.mat[i][j];
			}
		logger.debug("\nRELATIVE ERROR��%��:");
		try {
			logger.debug("RELATIVE ERROR��%��:\n");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DD.Print();
	}

	// ��ʼ�������еļ������?�?
	void init(int n, int r) {
		H = new Matrix_4_HSH(n, r);
		S = new Matrix_4_HSH(r, r);
		D = new Matrix_4_HSH(n, n);
		H.Rand();
		S.Rand();
		D.CreatSymMatrix();
	}

	void init(int n, int r, double[][] DistMat) {
		H = new Matrix_4_HSH(n, r);
		S = new Matrix_4_HSH(r, r);
		D = new Matrix_4_HSH(n, n);
		H.Rand();
		S.Rand();
		D.CopyMatrix(DistMat);
	}

	// factor_3_symmetricNMF, centralized
	public void symmetric_NMF(int n, int r, double[][] DistMat) {

		init(n, r, DistMat);

		// ����H��ת��
		Matrix_4_HSH H_ = new Matrix_4_HSH(r, n);
		// ����D��ת��
		// ����D������ɶԳƾ��󣬹�D��ת��D_��ͬ��D
		Matrix_4_HSH D_ = D;

		// �������е��м���
		Matrix_4_HSH temp1 = new Matrix_4_HSH(n, r);
		Matrix_4_HSH temp2 = new Matrix_4_HSH(n, r);
		Matrix_4_HSH temp3 = new Matrix_4_HSH(r, r);
		Matrix_4_HSH temp4 = new Matrix_4_HSH(r, r);

		// ��ʾ��ʼ����
		// logger.debug("D:");
		// D.Print();
		// logger.debug("\nBEFORE COMPUTATION:");
		// logger.debug("H:");
		// H.Print();
		// logger.debug("S:");
		// S.Print();

		// ��ʱ��ʼ
		long startTime = System.currentTimeMillis();
		// ������
		int iter = 0;
		final int COUNT = 2000;
		do {
			H_ = H.ToTranspose();
			// temp1=D_*H*S
			temp1 = D_.MatrixMultiply(H.MatrixMultiply(S));
			// temp2=H*H_*temp1
			temp2 = H.MatrixMultiply(H_.MatrixMultiply(temp1));
			// temp3=H_*D*H
			temp3 = H_.MatrixMultiply(D.MatrixMultiply(H));
			// temp4=H_*H*S*H_*H
			temp4 = H_.MatrixMultiply(H.MatrixMultiply(S.MatrixMultiply(H_
					.MatrixMultiply(H))));
			// H=H.*sqrt((D'*(H*S))./(H*H'*D'*(H*S)))
			if (H == null || S == null) {
				return;
			}
			H = H.DotMultiply((temp1.DotDivide(temp2)).Sqrt());
			// S=S.*sqrt((H'*D*H)./(H'*H*S*(H'*H)))
			S = S.DotMultiply((temp3.DotDivide(temp4)).Sqrt());

			iter++;
		} while (iter < COUNT);

		// ��ʾ������
		// logger.debug("\nAFTER COMPUTATION:");
		// logger.debug("H:");
		// H.Print();
		// logger.debug("S:");
		// S.Print();
		// ���龫��
		// logger.debug("\nCHECK ERROR:");
		D.CheckError();
		// ��ʱ����
		logger.debug("\nRUNNING TIME(ms):"
				+ (System.currentTimeMillis() - startTime));

	}

	public void distributed_NMF(int n, int r) {
		init(n, r);
		// ����H��ת��
		Matrix_4_HSH H_ = new Matrix_4_HSH(r, n);
		//
		double beta = 0.5;
		Matrix_4_HSH Beta = new Matrix_4_HSH(n, r);
		Matrix_4_HSH _Beta = new Matrix_4_HSH(n, r);
		for (int i = 0; i < n; i++)
			for (int j = 0; j < r; j++) {
				Beta.mat[i][j] = beta;
				_Beta.mat[i][j] = 1 - beta;
			}
		// �������е��м���
		Matrix_4_HSH temp1 = new Matrix_4_HSH(r, r);
		Matrix_4_HSH temp2 = new Matrix_4_HSH(r, r);
		Matrix_4_HSH temp3 = new Matrix_4_HSH(n, r);
		Matrix_4_HSH temp4 = new Matrix_4_HSH(n, r);

		// ��ʾ��ʼ����
		logger.debug("D:");
		D.Print();
		logger.debug("\nBEFORE COMPUTATION:");
		logger.debug("H:");
		H.Print();
		logger.debug("S:");
		S.Print();

		// ��ʱ��ʼ
		long startTime = System.currentTimeMillis();
		// ������
		int iter = 0;
		final int COUNT = 2000;
		do {
			H_ = H.ToTranspose();
			// temp1=S.*(H_*D*H)
			temp1 = S.DotMultiply(H_.MatrixMultiply(D.MatrixMultiply(H)));
			// temp2=H_*H*S*H_*H
			temp2 = H_.MatrixMultiply(H.MatrixMultiply(S.MatrixMultiply(H_
					.MatrixMultiply(H))));
			// temp3=beta*(D*(H*S))
			temp3 = Beta.DotMultiply(D.MatrixMultiply(H.MatrixMultiply(S)));
			// temp4=(H*S)*((H'*H)*S)
			temp4 = H.MatrixMultiply(S.MatrixMultiply(H_.MatrixMultiply(H
					.MatrixMultiply(S))));

			// S=temp1./temp2
			// S=S.*(H'*D*H)./(H'*H*S*(H'*H));
			S = temp1.DotDivide(temp2);
			// H=H.*(1-beta+temp3./temp4)
			// H=H.*(1-beta+beta*(D*(H*S))./((H*S)*((H'*H)*S)));
			H = H.DotMultiply(_Beta.DotAdd(temp3.DotDivide(temp4)));
			iter++;
		} while (iter < COUNT);

		// ��ʾ������
		logger.debug("\nAFTER COMPUTATION:");
		logger.debug("H:");
		H.Print();
		logger.debug("S:");
		S.Print();
		// ���龫��
		logger.debug("\nCHECK ERROR:");
		D.CheckError();
		// ��ʱ����
		logger.debug("\nRUNNING TIME(ms):"
				+ (System.currentTimeMillis() - startTime));
	}

	// ʵ��A\B������A���Ƿ������n*r��n>r��
	// BΪ����
	// ����AX=B�������Է�����⣬�õ���˹��Ԫ��?�?
	// ���÷���A.InverseDivide(B)
	Matrix_4_HSH LeftDivide(Matrix_4_HSH B) {
		// ��A��B�����||B��Ϊ����||A������С������϶���Ψһ��?�?
		if (mat_row != B.mat_row || B.mat_column != 1 || mat_row < mat_column) {
			logger.debug("��������������������");
			return null;
		}
		// ����������
		Matrix_4_HSH extendedMatrix = new Matrix_4_HSH(mat_row, mat_column + 1);
		for (int i = 0; i < mat_row; i++) {
			for (int j = 0; j < mat_column; j++)
				extendedMatrix.mat[i][j] = mat[i][j];
			extendedMatrix.mat[i][mat_column] = B.mat[i][0];
		}
		// ��ʱ�����ദ�õ�
		double temp = 0;
		// �б��?�?
		int p;
		// ����column-1�θ�˹��ȥ
		for (int k = 0; k < mat_column - 1; k++) {
			p = -1;
			// ����k�еĵ�һ�����Ԫ�أ������������У�p
			for (int m = k; m < mat_row; m++)
				if (extendedMatrix.mat[m][k] != 0) {
					p = m;
					break;
				}
			// ����k��һ�����Ԫ�ض�û�У���A���ȱض�С����column
			if (p == -1) {
				logger.debug("no (or no unique) solution exists");
				return null;
			}
			// ������p��k��Ԫ��
			if (p != k) {
				for (int j = 0; j <= mat_column; j++) {
					// ��4�����������е�}��
					temp = extendedMatrix.mat[p][j];
					extendedMatrix.mat[p][j] = extendedMatrix.mat[k][j];
					extendedMatrix.mat[k][j] = temp;
				}
			}
			// ���еؽ�����Ԫ
			for (int i = k + 1; i < mat_row; i++) {
				temp = extendedMatrix.mat[i][k] / extendedMatrix.mat[k][k];
				for (int j = k; j <= mat_column; j++)
					extendedMatrix.mat[i][j] -= temp * extendedMatrix.mat[k][j];
			}
		}
		// ����˹��Ԫ֮�󣬼��÷������Ƿ��н�
		// �б��?�?
		int t = mat_column - 1;
		boolean flag = false;
		do {
			// �����֡����������?�?-ϵ��������=1�������������޽�
			if (extendedMatrix.mat[t][mat_column - 1] == 0
					&& extendedMatrix.mat[t][mat_column] != 0) {
				logger.debug("no solution exists");
				return null;
			}
			// �����Ƿ����ȫ���У��ǣ��������м������
			else if (extendedMatrix.mat[t][mat_column - 1] == 0
					&& extendedMatrix.mat[t][mat_column] == 0)
				t++;
			// ���������}�����˵���˹��Ԫ֮����������������������������
			// һ�������b�Ϳ����do-whileѭ��
			else
				flag = true;
		} while (!flag && t < mat_row);
		// ������do-whileѭ����ԭ���ǣ��б��t���������������?�?
		// ˵���˹��Ԫ֮������������С�������������ⲻΨ�?�?
		if (t == mat_row) {
			logger.debug("no unique solution exists");
			return null;
		}
		// flag=true;flagΪ�������£��������?�?,ttΪ�б��?�?
		int tt = t;
		do {
			// ����˹��Ԫ֮���������?�?,������ڵ�t��֮��
			// ������}�������޹ص����������޽⣬��flagΪfalse
			if (extendedMatrix.mat[t][mat_column - 1]
					* extendedMatrix.mat[tt][mat_column] != extendedMatrix.mat[t][mat_column]
					* extendedMatrix.mat[tt][mat_column - 1])
				flag = false;
			++tt;
		} while (flag && tt < mat_row);

		if (!flag) {
			logger.debug("no solution exists");
			return null;
		}

		// �����ڸö�����
		Matrix_4_HSH result = new Matrix_4_HSH(1, mat_column);
		// ��ʼ�ش�
		result.mat[0][mat_column - 1] = extendedMatrix.mat[t][mat_column]
				/ extendedMatrix.mat[t][mat_column - 1];
		for (int i = mat_column - 2; i >= 0; i--) {
			temp = 0;
			for (int j = i; j < mat_column; j++)
				temp += extendedMatrix.mat[i][j] * result.mat[0][j];
			result.mat[0][i] = (extendedMatrix.mat[i][mat_column] - temp)
					/ extendedMatrix.mat[i][i];
		}
		return result;
	}

	/*
	 * //distributed_newhosts_symmetricNMF //new_out,new_in Distance from host
	 * to landamrks //exist_lam indexes of measured landmarks public Matrix
	 * D_N_S_NMF(Matrix host2Landmark){ if(H.row!=host2Landmark.column) return
	 * null; Matrix temp1=new Matrix(S.row,H.row); Matrix temp2=new
	 * Matrix(H.row,S.row); Matrix temp3=new Matrix(host2Landmark.column,1);
	 * Matrix temp4=new Matrix(1,host2Landmark.column);
	 * 
	 * temp1=S.MatrixMultiply(H.ToRank()); temp2=temp1.ToRank();
	 * 
	 * Matrix result=new Matrix(host2Landmark.row,host2Landmark.column);
	 * 
	 * for(int i=0;i<host2Landmark.row;i++){ for(int
	 * j=0;j<host2Landmark.column;j++) temp3.mat[j][0]=host2Landmark.mat[i][j];
	 * temp4=temp2.LeftDivide(temp3); if(temp4!=null){ for(int
	 * m=0;m<host2Landmark.column;m++) result.mat[i][m]=temp4.mat[0][m]; } }
	 * return result; }
	 */

	// ����������A����ͨ�����б任�任ΪHermite��׼��B����B��ǰr�������޹�
	// ���÷���A.Hermite����
	private Matrix_4_HSH Hermite() {
		Matrix_4_HSH result = new Matrix_4_HSH(mat_row, mat_column);
		for (int i = 0; i < mat_row; i++)
			for (int j = 0; j < mat_column; j++)
				result.mat[i][j] = mat[i][j];
		// ��
		double var;
		//
		int temp = -1;
		int rowMark = -1;
		int columnMark = -1;
		//
		boolean flag;
		// ��ʼ��Ԫ
		for (int k = 0; k < mat_column; k++) {
			flag = false;
			// �ڵ�k�в��ҵ�һ����ֵķ��㣬���������У���¼��temp��
			for (int m = rowMark + 1; m < mat_row; m++)
				if (result.mat[m][k] != 0) {
					temp = m;
					flag = true;
					rowMark++;
					columnMark++;
					break;
				}
			// ���k��û�з���Ԫ����
			if (!flag) {
				columnMark++;
				// ��Ԫ�Ѿ��������һ��?�?
				if (k == mat_column - 1) {
					return result;
				}
				continue;
			}
			if (temp != rowMark) {
				for (int j = 0; j < mat_column; j++) {
					// ��4��������е�}��
					var = result.mat[temp][j];
					result.mat[temp][j] = result.mat[rowMark][j];
					result.mat[rowMark][j] = var;
				}
			}
			// ��׼����rowMark��
			for (int j = columnMark + 1; j < mat_column; j++)
				result.mat[k][j] /= result.mat[rowMark][columnMark];
			result.mat[rowMark][columnMark] = 1;
			// �������У����еؽ�����Ԫ
			for (int i = 0; i < mat_row; i++) {
				if (i == rowMark)
					continue;
				var = result.mat[i][k];
				for (int j = columnMark; j < mat_column; j++)
					result.mat[i][j] -= var * result.mat[rowMark][j];
			}
		}
		return result;
	}

	// �÷�����Ҫ����������A��Moore_Penrose�����A+��
	// �������A��Hermite��׼��B
	// ��ݶ��?����B���ɽ�A�������ȷֽ⣬�ֽ�ΪF��G
	// �ٸ�ݹ�ʽA+=G__*Inverse(G*G__)*Inverse(F__*F)*F__������G__��F__�ֱ��ʾG��F�Ĺ���ת��
	// ���÷�����A+��=A.Moore-PenroseInverse();
	public Matrix_4_HSH Moore_PenroseInverse() {
		Matrix_4_HSH B = new Matrix_4_HSH(mat_row, mat_column);
		B = Hermite();
		// ��¼B����
		int count = 0;
		// ���B�е�һ�в�ȫΪ�㣬flag��Ϊtrue
		boolean flag;
		for (int i = 0; i < mat_row; i++) {
			flag = false;
			for (int j = i; j < mat_column; j++) {
				if (B.mat[i][j] != 0) {
					count++;
					flag = true;
					continue;
				}
			}
			// B�е�i��ȫΪ��ʱ
			if (!flag)
				break;
		}
		int[] record = new int[count];

		for (int i = 0; i < count; i++)
			for (int j = i; j < mat_column; j++)
				if (B.mat[i][j] != 0)
					record[i] = j;
		// ȡFΪA��J1��J2��...��Jr�й���row*count����
		Matrix_4_HSH F = new Matrix_4_HSH(mat_row, count);
		for (int j = 0; j < count; j++) {
			int t = record[j];
			for (int i = 0; i < mat_row; i++)
				F.mat[i][t] = mat[i][t];
		}
		// GΪB��ǰr�й���count*column����
		Matrix_4_HSH G = new Matrix_4_HSH(count, mat_column);
		for (int i = 0; i < count; i++) {
			for (int j = 0; j < mat_column; j++)
				G.mat[i][j] = B.mat[i][j];
		}
		// ����A��Moore-Penrose�����A+��
		// A+=G__*Inverse(G*G__)*Inverse(F__*F)*F__
		// ����F��G��Ϊʵ�����乲��ת�þ�����ת�þ���
		Matrix_4_HSH result = new Matrix_4_HSH(mat_column, mat_row);
		// temp1=G_*Inverse(G*G_)
		Matrix_4_HSH temp1 = new Matrix_4_HSH(mat_column, count);
		temp1 = (G.ToTranspose()).MatrixMultiply((G.MatrixMultiply(G.ToTranspose()))
				.Inverse());

		// temp2=Inverse(F_*F)*F_
		Matrix_4_HSH temp2 = new Matrix_4_HSH(count, mat_row);
		temp2 = (((F.ToTranspose()).MatrixMultiply(F)).Inverse()).MatrixMultiply(F
				.ToTranspose());
		// ��A+��=temp1*temp2
		result = temp1.MatrixMultiply(temp2);

		return result;
	}

	// distributed_newhosts_symmetricNMF
	// new_out,new_in Distance from host to landamrks
	// exist_lam indexes of measured landmarks
	// right=S*H';
	// for i=1:M
	// t=right'\host2Landmark(i,1:N)';
	// new_h(i,:)=t';
	public Matrix_4_HSH D_N_S_NMF(Matrix_4_HSH host2Landmark) {

		if (H.mat_row != host2Landmark.mat_column)
			return null;
		// right=S*H'
		Matrix_4_HSH temp1 = new Matrix_4_HSH(S.mat_row, H.mat_row);
		// right'
		Matrix_4_HSH temp2 = new Matrix_4_HSH(H.mat_row, S.mat_row);
		// ����
		Matrix_4_HSH temp3 = new Matrix_4_HSH(host2Landmark.mat_column, 1);
		// ����
		Matrix_4_HSH temp4 = new Matrix_4_HSH(1, S.mat_row);
		// right��Moore_PenroseInverse�����?�?
		Matrix_4_HSH temp5 = new Matrix_4_HSH(S.mat_row, H.mat_row);

		temp1 = S.MatrixMultiply(H.ToTranspose());
		temp2 = temp1.ToTranspose();
		temp5 = temp2.Moore_PenroseInverse();

		Matrix_4_HSH result = new Matrix_4_HSH(host2Landmark.mat_row, S.mat_row);

		for (int i = 0; i < host2Landmark.mat_row; i++) {
			for (int j = 0; j < host2Landmark.mat_column; j++)
				temp3.mat[j][0] = host2Landmark.mat[i][j];
			temp4 = (temp5.MatrixMultiply(temp3)).ToTranspose();
			if (temp4 != null) {
				for (int m = 0; m < S.mat_row; m++)
					result.mat[i][m] = temp4.mat[0][m];
			}
		}
		return result;
	}

	public Matrix_4_HSH D_N_S_NMF(Matrix_4_HSH host2Landmark, 
			Matrix_4_HSH _H, Matrix_4_HSH _S) {

		if (_H.mat_row != host2Landmark.mat_column)
			return null;
		// right=S*H'
		Matrix_4_HSH temp1 = new Matrix_4_HSH(_S.mat_row, _H.mat_row);
		// right'
		Matrix_4_HSH temp2 = new Matrix_4_HSH(_H.mat_row, _S.mat_row);
		// ����
		Matrix_4_HSH temp3 = new Matrix_4_HSH(host2Landmark.mat_column, 1);
		// ����
		Matrix_4_HSH temp4 = new Matrix_4_HSH(1, _S.mat_row);
		// right��Moore_PenroseInverse�����?�?
		Matrix_4_HSH temp5 = new Matrix_4_HSH(_S.mat_row, _H.mat_row);

		temp1 = _S.MatrixMultiply(_H.ToTranspose());
		temp2 = temp1.ToTranspose();
		temp5 = temp2.Moore_PenroseInverse();

		Matrix_4_HSH result = new Matrix_4_HSH(host2Landmark.mat_row, _S.mat_row);

		for (int i = 0; i < host2Landmark.mat_row; i++) {
			for (int j = 0; j < host2Landmark.mat_column; j++)
				temp3.mat[j][0] = host2Landmark.mat[i][j];
			temp4 = (temp5.MatrixMultiply(temp3)).ToTranspose();
			if (temp4 != null) {
				for (int m = 0; m < _S.mat_row; m++)
					result.mat[i][m] = temp4.mat[0][m];
			}
		}
		return result;
	}

	
	/**
	 * transform a boolean vector
	 * @param _H
	 * @return
	 */
	public boolean[] transformed2BinaryVectors(double[][] _H){
		if(_H==null){
			return null;
		}
		int len=_H.length;
		if(len==0){
			return null;
		}
		boolean[] bits=new boolean[len];
		
		if(_H[0].length!=2){
			assert(false);
		}
		for(int i=0;i<len;i++){
			//first dimension
			if(_H[i][0]>_H[i][1]){
				bits[i]=true;
			}else{
				bits[i]=false;
			}
		}
		return bits;
	
	}
	
	public boolean FG_NMF(int _row, int _column, 
			int _r, double[][] relativeCoord, double[][] _F,
			double[][] _G){
		//clear();
		
		Matrix_4_HSH temp1;
		Matrix_4_HSH temp2;
		Matrix_4_HSH temp3;
		Matrix_4_HSH temp4;
		Matrix_4_HSH temp5;
		Matrix_4_HSH temp6;
		
		Matrix_4_HSH H_Transpose;
		
		
		H = new Matrix_4_HSH(_row, _r);  //F
		S = new Matrix_4_HSH(_column, _r); //G
		D = new Matrix_4_HSH(_row, _column);
		H.Rand();
		S.Rand();
		D.CopyMatrix(relativeCoord);
	

		

	

		Matrix_4_HSH D_Transpose = D.ToTranspose();
		
		if(D_Transpose==null){
			System.err.println("$D_Transpose is null ");
			return false;
		}

		if(H==null){
			System.err.println("H is null! ");			
			return false;
		}		
		
		
		/*H.Print();
		S.Print();
		D.Print();
		D_Transpose.Print();*/
		
	
		int iter = 0;
		int COUNT = 2000;
		
	
		do {
			
			temp1=D_Transpose.MatrixMultiply(H);
			if(temp1==null){
				System.err.println("temp1 is null!");
				//System.exit(-1);
				return false;
			}
			H_Transpose=H.ToTranspose();
			temp4=H_Transpose.MatrixMultiply(H);
			if(temp4==null){
				System.err.println("temp4 is null!");
				return false;
			}
			temp2=S.MatrixMultiply(temp4);	
			
			temp3=D.MatrixMultiply(S);
			temp5=temp3.DotDivide(H.MatrixMultiply(H_Transpose).MatrixMultiply(temp3));
			
			if(H==null||S==null||temp5==null){
				logger.debug("null results!");
				return false;
			}

			temp6=temp5.Sqrt();
			
			if(temp1==null||temp2==null||temp6==null){
				logger.debug("null matrix!");
				return false;
			}

			//==========================
			
			S=S.DotMultiply(temp1.DotDivide(temp2));
			H=H.DotMultiply(temp6);
			//logger.debug("$iter "+iter);

			iter++;
			
		}while(iter<COUNT);
		

		for (int i = 0; i < _row; i++)
			for (int j = 0; j < _r; j++)
				_F[i][j] = H.mat[i][j];
		for (int i = 0; i < _column; i++)
			for (int j = 0; j < _r; j++)
				_G[i][j] = S.mat[i][j];

		return true;
	}
	/**
	 * HSH
	 * @param n
	 * @param r
	 * @param DistMat
	 * @param _H
	 * @param _S
	 */
	void symmetric_NMF(int n, int r, double[][] DistMat, double[][] _H,
			double[][] _S) {

		clear();

		H = new Matrix_4_HSH(n, r);
		S = new Matrix_4_HSH(r, r);
		D = new Matrix_4_HSH(n, n);
		H.Rand();
		S.Rand();
		D.CopyMatrix(DistMat);
		logger.debug("\n:@@Latency matrix:\n");

		D.Print();

		Matrix_4_HSH H_;

		Matrix_4_HSH D_ = D;

		Matrix_4_HSH temp1;
		Matrix_4_HSH temp2;
		Matrix_4_HSH temp3;
		Matrix_4_HSH temp4;
		Matrix_4_HSH temp5;
		Matrix_4_HSH temp6;

		long startTime = System.currentTimeMillis();

		int iter = 0;
		final int COUNT = 2000;
		do {
			H_ = H.ToTranspose();
			// temp1=D_*H*S
			temp1 = D_.MatrixMultiply(H.MatrixMultiply(S));
			// temp2=H*H_*temp1
			temp2 = H.MatrixMultiply(H_.MatrixMultiply(temp1));
			// temp3=H_*D*H
			temp3 = H_.MatrixMultiply(D.MatrixMultiply(H));
			// temp4=H_*H*S*H_*H
			temp4 = H_.MatrixMultiply(H.MatrixMultiply(S.MatrixMultiply(H_
					.MatrixMultiply(H))));
			// H=H.*sqrt((D'*(H*S))./(H*H'*D'*(H*S)))
			if (H == null || S == null) {
				return;
			}
			temp5 = temp1.DotDivide(temp2);
			temp6 = temp3.DotDivide(temp4);
			if (temp5 == null || temp6 == null) {
				return;
			}
			H = H.DotMultiply(temp5.Sqrt());
			// S=S.*sqrt((H'*D*H)./(H'*H*S*(H'*H)))
			S = S.DotMultiply(temp6.Sqrt());

			iter++;
		} while (iter < COUNT);

		for (int i = 0; i < n; i++)
			for (int j = 0; j < r; j++)
				_H[i][j] = H.mat[i][j];
		for (int i = 0; i < r; i++)
			for (int j = 0; j < r; j++)
				_S[i][j] = S.mat[i][j];

		logger.debug("Current Time: ");
		long perSec = (long) Math.pow(10, 3);
		logger.debug("$: "+ (System.currentTimeMillis())
				/ perSec);
		logger.debug("D:");

		D.Print();

		logger.debug("\nAFTER COMPUTATION:");
		try {
			logger.debug("\nAFTER COMPUTATION:\n");
			logger.debug("H:");
			logger.debug("H:\n");
			H.Print();
			logger.debug("S:");
			logger.debug("S:\n");
			S.Print();

			// logger.debug("\nCHECK ERROR:");
			// logger.debug("\nCHECK ERROR:\n");
			// CheckError();

			long time = (System.currentTimeMillis() - startTime);
			logger.debug("\nRUNNING TIME(ms):" + time);
			logger.debug("\nRUNNING TIME(ms):" + time + "\n");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * find accurate clustering number
	 * 
	 * @param distanceMatrix
	 * @return
	 */
	public int findClusteringNum(double[][] distanceMatrix,
			double[][] optHMat,double[][] optSMat,int lowCluster, int highCluster) {

		// TODO Auto-generated method stub
		int NoOfLandmarks = distanceMatrix.length;
		double SCValue = Double.MIN_VALUE;
		int clusterNumber = -1;
		double[] SC;
		
		double[][] optH = null;
		double[][] optS = null;
		
		for (int cluNum = lowCluster; cluNum <= highCluster; cluNum++) {

			// update coordinate vectors

			/*
			 * init_SymmetricNMF(NoOfLandmarks,DIMS_NUM);
			 * D.CopyMatrix(distanceMatrix);
			 * 
			 * symmetric_NMF(D,DIMS_NUM);
			 */
			double[][] H11 = new double[NoOfLandmarks][cluNum];
			double[][] S11 = new double[cluNum][cluNum];

			// CurList.Coord = new double[cluNum];
			for (int i = 0; i < NoOfLandmarks; i++)
				for (int j = 0; j < cluNum; j++) {
					H11[i][j] = 0;
				}
			for (int i = 0; i < cluNum; i++)
				for (int j = 0; j < cluNum; j++) {
					S11[i][j] = 0;
				}

			symmetric_NMF(NoOfLandmarks, cluNum, distanceMatrix, H11, S11);
			SC = Silhouette_Coefficient(H);
			// use middle value to verify the clustering quality
			Arrays.sort(SC);
			double middle = SC[Math.round(NoOfLandmarks / 2)];
			if (SCValue < middle) {
				SCValue = middle;
				clusterNumber = cluNum;
				optH = H11;
				optS = S11;
			}


		}
		//save
		optHMat = optH;
		optSMat = optS;
		
		return clusterNumber;
	}

	void init_SymmetricNMF(int nodesNum, int DIMS_NUM) {

		H = new Matrix_4_HSH(nodesNum, DIMS_NUM);
		S = new Matrix_4_HSH(DIMS_NUM, DIMS_NUM);
		D = new Matrix_4_HSH(nodesNum, nodesNum);
		H.Rand();
		S.Rand();

	}

	/*
	 * void symmetric_NMF(Matrix landmark2Landmark,int DIMS_NUM){
	 * 
	 * //init(); int LANDMARK_NUM=landmark2Landmark.row;
	 * 
	 * Matrix landmark2Landmark_ = new Matrix(LANDMARK_NUM,LANDMARK_NUM);
	 * landmark2Landmark_=landmark2Landmark.ToRank();
	 * 
	 * Matrix H_ = new Matrix(DIMS_NUM ,LANDMARK_NUM);
	 * 
	 * long startTime=System.currentTimeMillis(); int iter=0; final int
	 * COUNT=2000; do{ H_=H.ToRank();
	 * 
	 * H=H.DotMultiply((landmark2Landmark_.MatrixMultiply(H.MatrixMultiply(S)).
	 * DotDivide
	 * (H.MatrixMultiply(H_.MatrixMultiply(landmark2Landmark_.MatrixMultiply
	 * (H.MatrixMultiply(S)))))).Sqrt());
	 * 
	 * S=S.DotMultiply((H_.MatrixMultiply(landmark2Landmark.MatrixMultiply(H)).
	 * DotDivide
	 * (H_.MatrixMultiply(H.MatrixMultiply(S.MatrixMultiply(H_.MatrixMultiply
	 * (H)))))).Sqrt()); iter++; }while(iter<COUNT);
	 * 
	 * 
	 * logger.debug("\nRUNNING TIME(s):"+(System.currentTimeMillis()-startTime
	 * )/1000); }
	 */

	double[] Silhouette_Coefficient(Matrix_4_HSH landmarkCoords) {
		int nodesNum = landmarkCoords.mat_row;
		int nodes2Cluster[] = new int[nodesNum];
		nodes2Cluster = divideCluster(landmarkCoords);

		logger.debug("nodes2Cluster : ");// /////////////////////
		for (int i = 0; i < nodesNum; i++)
			// ////////////////////////////////
			System.out.print(nodes2Cluster[i] + "\t");// ////////////////
	
		double a, b;
		double SC[] = new double[nodesNum];
		for (int nodeNo = 0; nodeNo < nodesNum; nodeNo++) {
			a = intraClusterDist(landmarkCoords, nodes2Cluster, nodeNo);
			b = interClusterDist(landmarkCoords, nodes2Cluster, nodeNo);
			// //logger.debug("a = " + a);//////////////////////
			// //logger.debug("b = " + b);////////////////////////
			SC[nodeNo] = (b - a) / Math.max(a, b);
			// //logger.debug("SC[" + nodeNo+ "] =" + SC[nodeNo]);

		}
		return SC;
	}

	int[] divideCluster(Matrix_4_HSH coordsMatrix) {
		int nodesNum = coordsMatrix.mat_row;
		// record the cluster No. in int[] nodes2Cluster
		int nodes2Cluster[] = new int[nodesNum];

		int DIMS_NUM = coordsMatrix.mat_column;
		for (int i = 0; i < nodesNum; i++) {
			double max = Math.abs(coordsMatrix.mat[i][0]);
			nodes2Cluster[i] = 0;
			for (int j = 1; j < DIMS_NUM; j++)
				if (max < Math.abs(coordsMatrix.mat[i][j])) {
					max = Math.abs(coordsMatrix.mat[i][j]);
					nodes2Cluster[i] = j;
				}
		}
		return nodes2Cluster;
	}

	// according to the network coordinates, compute the average distance from
	// one certain node to nodes within the same cluster
	double intraClusterDist(Matrix_4_HSH coordsMatrix, int[] nodes2Cluster, int nodeNo) {
		int clusterNo = nodes2Cluster[nodeNo];
		int nodesNum = coordsMatrix.mat_row;

		int DIMS_NUM = coordsMatrix.mat_column;
		Matrix_4_HSH x = new Matrix_4_HSH(1, DIMS_NUM);
		for (int j = 0; j < DIMS_NUM; j++)
			x.mat[0][j] = coordsMatrix.mat[nodeNo][j];
		Matrix_4_HSH y = new Matrix_4_HSH(DIMS_NUM, 1);
		int count = 0;// the Sum of other nodes in the cluster except the node
						// itself
		double dist = 0;

		// test not -1
		double testNeg = 0;
		for (int i = 0; i < nodesNum; i++)
			if (nodes2Cluster[i] == clusterNo && i != nodeNo) {
				for (int j = 0; j < DIMS_NUM; j++)
					y.mat[j][0] = coordsMatrix.mat[i][j];
				testNeg = x.MatrixMultiply(S.MatrixMultiply(y)).mat[0][0];
				if (testNeg > 0) {
					dist += testNeg;
				} else {
					continue;
				}
				count++;
			}
		if (count != 0)
			return dist / count;
		return 0;
	}

	// according to the network coordinates, compute the average distance 
	//from
	// one certain node to nodes outside the same cluster
	double interClusterDist(Matrix_4_HSH coordsMatrix, int[] nodes2Cluster, int nodeNo) {
		int clusterNo = nodes2Cluster[nodeNo];
		int nodesNum = coordsMatrix.mat_row;
		int DIMS_NUM = coordsMatrix.mat_column;

		int otherClusterNo[] = new int[getClusterSum(nodes2Cluster) - 1];
		otherClusterNo = getOtherClusterNo(nodes2Cluster, clusterNo);

		Matrix_4_HSH x = new Matrix_4_HSH(1, DIMS_NUM);
		for (int j = 0; j < DIMS_NUM; j++)
			x.mat[0][j] = coordsMatrix.mat[nodeNo][j];
		Matrix_4_HSH y = new Matrix_4_HSH(DIMS_NUM, 1);
		int count;// the quantity of other nodes in "clusterNo" cluster
		double dist_;
		double dist = 1000000;// Infinity

		double testNeg = 0;

		for (int k = 0; k < otherClusterNo.length; k++) {
			count = 0;
			dist_ = 0;
			for (int i = 0; i < nodesNum; i++)
				if (nodes2Cluster[i] == otherClusterNo[k]) {
					for (int j = 0; j < DIMS_NUM; j++) {
						y.mat[j][0] = coordsMatrix.mat[i][j];
					}
					testNeg = x.MatrixMultiply(S.MatrixMultiply(y)).mat[0][0];
					if (testNeg > 0) {
						dist_ += testNeg;
					} else {
						continue;
					}
					count++;
				}
			dist_ /= count;
			dist = dist < dist_ ? dist : dist_;
		}
		return dist;
	}

	// return the quantity of clusters
	int getClusterSum(int[] nodes2Cluster) {
		int clusterSum = 0;// record the quantity of clusters
		int nodesNum = nodes2Cluster.length;
		int tmp[] = new int[nodesNum];
		// copy from nodes2Cluster
		System.arraycopy(nodes2Cluster, 0, tmp, 0, nodesNum);
		int temp = -1;
		for (int i = 0; i < nodesNum; i++)
			if (tmp[i] != -1) {
				temp = tmp[i];
				for (int j = i; j < nodesNum; j++)
					if (temp == tmp[j])
						tmp[j] = -1;
				clusterSum++;
			}
		return clusterSum;
	}

	// get the clusters' No. which the node does not belong to
	int[] getOtherClusterNo(int[] nodes2Cluster, int clusterNo) {
		int clusterSum = getClusterSum(nodes2Cluster);
		int nodesNum = nodes2Cluster.length;
		int tmp[] = new int[nodesNum];
		// copy from nodes2Cluster
		System.arraycopy(nodes2Cluster, 0, tmp, 0, nodesNum);
		for (int i = 0; i < nodesNum; i++)
			if (tmp[i] == clusterNo)
				tmp[i] = -1;
		int otherClusterNo[] = new int[clusterSum - 1];
		int temp = -1;
		for (int i = 0; i < nodesNum && temp < clusterSum - 1; i++)
			if (tmp[i] != -1) {
				otherClusterNo[++temp] = tmp[i];
				for (int j = i; j < nodesNum; j++)
					if (otherClusterNo[temp] == tmp[j])
						tmp[j] = -1;
			}
		return otherClusterNo;
	}

	public void clear() {
		H = null;
		D = null;
		S = null;
		mat = null;
	}
}