package org.littlestar.tpcc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.BitSet;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

public final class RandomHelper {
	private RandomHelper() {}
	
	/**
	 * 返回在min和max之间的随机整数值.
	 * @param min 返回随机数的最小值(包含min值);
	 * @param max 返回随机数的最大值(包含max值);
	 * @return 返回在min和max之间的随机整数值.
	 */
	public static int randomInt(int min, int max) {
		if(max < Integer.MAX_VALUE)
			max ++;
		return ThreadLocalRandom.current().nextInt(min, max);
	}
	
	public static long randomLong(long min, long max) {
		if(max < Long.MAX_VALUE)
			max ++;
		return ThreadLocalRandom.current().nextLong(min, max);
	}
	
	public static Date randomTime(Date min, Date max) {
		long time = randomLong(min.getTime(), max.getTime());
		return new Date(time);
	}
	
	/**
	 * 
	 * @param min
	 * @param max the max bound (exclusive)
	 * @return
	 */
	public static double randomDouble(double min, double max) {
		return ThreadLocalRandom.current().nextDouble(min, max);
	}
	
	public static double randomDouble(double max) {
		return ThreadLocalRandom.current().nextDouble(max); //bound
	}
	
	public static double randomDouble() {
		return ThreadLocalRandom.current().nextDouble();
	}
	
	/**
	 * 
	 * @param precision Total number of significant digits.
	 * @param scale Number of digits to the right of the decimal point
	 * @return
	 */
	public static double randomDecimal(int precision, int scale) {
		double bound = getBound(precision, scale);
		double randD = randomDouble(bound);
		BigDecimal decimal = new BigDecimal(randD).setScale(scale, RoundingMode.HALF_UP);
		return decimal.doubleValue();
	}
	
	public static double randomDecimal(int scale, double min, double max) {
		double randD = randomDouble(min, max);
		BigDecimal decimal = new BigDecimal(randD).setScale(scale, RoundingMode.HALF_UP);
		return decimal.doubleValue();
	}
	
	/**
	 * 获取指定范围和精度的最大值, 笨办法, 构造字符串, 再转成双精。
	 */
	static double getBound(int precision, int scale) {
		int lLen = precision - scale;
		int rLen = scale;
		String strBound;
		StringBuilder lPart = new StringBuilder();
		if (lLen < 0) {
			throw new IllegalArgumentException(
					"unexpected value: precision(" + precision + ") < scale(" + scale + ").");
		} else if (lLen == 0) {
			lPart.append("0");
		} else {
			for (int i = 0; i < lLen; i++) {
				lPart.append("9");
			}
		}
		if (rLen > 0) {
			StringBuilder rPart = new StringBuilder();
			for (int i = 0; i < rLen; i++) {
				rPart.append("9");
			}
			strBound = lPart.toString() + "." + rPart.toString();
		} else {
			strBound = lPart.toString();
		}
		return Double.parseDouble(strBound);
	}
	
	public static float randomFloat() {
		return ThreadLocalRandom.current().nextFloat();
	}
	
	public static boolean randomBoolean() {
		return ThreadLocalRandom.current().nextBoolean();
	}
	
	public static String randomString(int length, String characters) {
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			int index = randomInt(0, characters.length()-1);
			sb.append(characters.charAt(index));
		}
		return sb.toString();
	}
	
	public static String randomString(int length) {
		String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"   //Upper Case Letters 
						  + "abcdefghijklmnopqrstuvxyz"   //Lower Case Letters
						  + "0123456789"                  //Numbers
						  //+ "*@#+%"                     //Special Symbols
						  ;                          
		return randomString(length, characters);
	}
	
	public static String randomString(int minLen, int maxLen) {
		int length = randomInt(minLen, maxLen);
		return randomString(length);
	}
	
	public static String randomNumberString(int length) {
		String characters = "0123456789";
		return randomString(length, characters);
	}
	
	/**
	 * 生成一个随机置位的位图(BitSet)。
	 * @param setBitCount 随机置位的数量.
	 * @param mapSize 位图大小(实际大小时64的倍数, 请参考BitSet的api文档).
	 * @return 返回随机置位的位图。
	 */
	public static BitSet randomBitMap(int setBitCount, int mapSize) {
		if (setBitCount >= mapSize)
			throw new IllegalArgumentException(
					"setBitCount=" + setBitCount + " must less than mapSize=" + mapSize + " .");
		BitSet bitmap = new BitSet(mapSize);
		while (bitmap.cardinality() < setBitCount) {
			int index = randomInt(0, mapSize + 1);
			bitmap.set(index);
		}
		return bitmap;
	}
	
	
	/**
	 * 非一致性随机算法: non-uniform random (NURand) 
	 * 
	 * @param a
	 * @param x
	 * @param y
	 * @return
	 */
	public static int nuRand(int a, int x, int y) {
		int c_255 = randomInt(0, 255);
		int c_1023 = randomInt(0, 1023);
		int c_8191 = randomInt(0, 8191);
		int c = 0;
		switch (a) {
		case 255:
			c = c_255;
			break;
		case 1023:
			c = c_1023;
			break;
		case 8191:
			c = c_8191;
			break;
		default:
			throw new IllegalArgumentException("NURand: unexpected value (" + a + ") of A used. ");
		}
		return (((randomInt(0, a) | randomInt(x, y)) + c) % (y - x + 1)) + x;
	}
	
	public static int[] randomPermutation(int size) {
		int[] nums = new int[size];
		// 初始化, 数组值顺序排列: 1,2,3,4,... size;
		for (int i = 0; i < size; i++) {
			nums[i] = i + 1;
		}
		// 洗牌, 将数组值乱序排列: 10,3,1,7,...
		for (int i = 0; i < nums.length; i++) {
			int j = RandomHelper.randomInt(i, size-1);
			int t = nums[i];
			nums[i] = nums[j];
			nums[j] = t;
		}
		return nums;
	}
	
	public static String lastName(int num) {
		String name = null;
		String[] n = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
		name = n[num / 100];
		name = name + n[(num / 10) % 10];
		name = name + n[num % 10];
		return name;
	}
	
	// public static final float newOrderRatio = 0.45f;
	public static final float paymentRation     = 0.43f;
	public static final float orderStatusRation = 0.04f;
	public static final float deliveryRation    = 0.04f;
	public static final float stockLevelRation  = 0.04f;
	
	public static TransactionType randomTransaction(float paymentRation, float orderStatusRation, float deliveryRation,
			float stockLevelRation) {
		float chance = RandomHelper.randomFloat();
		if (paymentRation >= chance) {
			return TransactionType.Payment;
		} else if (paymentRation < chance && chance <= (paymentRation + orderStatusRation)) { 
			return TransactionType.OrderStatus;
		} else if ((paymentRation + orderStatusRation) < chance
				&& chance <= (paymentRation + orderStatusRation + deliveryRation)) { 
			return TransactionType.Delivery;
		} else if ((paymentRation + orderStatusRation + deliveryRation) < chance
				&& chance <= (paymentRation + orderStatusRation + deliveryRation + stockLevelRation)) { 
			return TransactionType.StockLevel;
		} else {
			return TransactionType.NewOrder;
		}
	}
	
	public static TransactionType randomTransaction() {
		return randomTransaction(paymentRation, orderStatusRation, deliveryRation, stockLevelRation);
	}
	
}
