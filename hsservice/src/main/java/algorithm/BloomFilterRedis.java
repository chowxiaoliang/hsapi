package algorithm;

import redis.clients.jedis.Jedis;
import redis.clients.util.Hashing;

/**
 * 结合redis实现的布隆过滤器
 */
public class BloomFilterRedis {

    // 要插入多少数据
    static final int expectedInsertions = 1000;

    // 期望的误判率
    static final double fpp = 0.01;

    // bit数组长度
    private static long numBits;

    // hash函数数量
    private static int numHashFunctions;

    static {
        numBits = optimalNumOfBits(expectedInsertions, fpp);
        numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
    }

    /**
     * 根据key获取bitmap下标
     */
    private static long[] getIndexArray(String key) {
        long hash1 = hash(key);
        long hash2 = hash1 >>> 16;
        long[] result = new long[numHashFunctions];
        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            result[i] = combinedHash % numBits;
        }
        return result;
    }

    private static long hash(String key) {
        return Hashing.MURMUR_HASH.hash(key);
    }

    // 计算hash函数个数
    private static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    // 计算bit数组长度
    private static long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    static {
        numBits = optimalNumOfBits(expectedInsertions, fpp);
        numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
    }


    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.9.31", 6379);
        for (int i = 0; i < 1000; i++) {
            long[] indexArray = getIndexArray(String.valueOf(i));
            for (long index : indexArray) {
                jedis.setbit("codebear:bloom", index, true);
            }
        }
        int num = 0;
        for (int i = 1000; i < 2000; i++) {
            long[] indexArray = getIndexArray(String.valueOf(i));
            for (long index : indexArray) {
                if (!jedis.getbit("codebear:bloom", index)) {
                    System.out.println(i + "一定不存在");
                    num++;
                    break;
                }
            }
        }
        System.out.println("一定不存在的有" + num + "个");
    }
}
