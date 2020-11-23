package spark.core.variable;

import org.apache.spark.util.AccumulatorV2;

import java.math.BigInteger;

/**
 * 自定义累加器
 * @author zhouliang
 */
public class SelfDeAccumulator {

    public static class BigIntegerAccumulator extends AccumulatorV2<BigInteger, BigInteger>{

        private BigInteger num = BigInteger.ZERO;

        public BigIntegerAccumulator(){}

        public BigIntegerAccumulator(BigInteger bigInteger){
            num = new BigInteger(bigInteger.toString());
        }

        @Override
        public boolean isZero() {
            return BigInteger.ZERO.compareTo(num) == 0;
        }

        @Override
        public AccumulatorV2<BigInteger, BigInteger> copy() {
            return new BigIntegerAccumulator(num);
        }

        @Override
        public void reset() {
            num = BigInteger.ZERO;
        }

        @Override
        public void add(BigInteger v) {
            num = num.add(v);
        }

        @Override
        public void merge(AccumulatorV2<BigInteger, BigInteger> other) {
            num = num.add(other.value());
        }

        @Override
        public BigInteger value() {
            return num;
        }
    }
}
