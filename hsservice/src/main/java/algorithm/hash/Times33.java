package algorithm.hash;

/**
 * 1. << : 左移运算符，num << 1,相当于num乘以2  低位补02. >> : 右移运算符，num >> 1,相当于num除以2  高位补03. >>> : 无符号右移，忽略符号位，空位都以0补齐4. % : 模运算 取余5. ^ : 位异或 第一个操作数的的第n位于第二个操作数的第n位相反，那么结果的第n为也为1，否则为06. & : 与运算 第一个操作数的的第n位于第二个操作数的第n位如果都是1，那么结果的第n为也为1，否则为07. | : 或运算 第一个操作数的的第n位于第二个操作数的第n位 只要有一个是1，那么结果的第n为也为1，否则为08. ~ : 非运算 操作数的第n位为1，那么结果的第n位为0，反之，也就是取反运算（一元操作符：只操作一个数）
 * 3. >>> : 无符号右移，忽略符号位，空位都以0补齐
 * 4. % : 模运算 取余
 * 5. ^ : 位异或 第一个操作数的的第n位于第二个操作数的第n位相反，那么结果的第n为也为1，否则为0
 * 6. & : 与运算 第一个操作数的的第n位于第二个操作数的第n位如果都是1，那么结果的第n为也为1，否则为0
 * 7. | : 或运算 第一个操作数的的第n位于第二个操作数的第n位 只要有一个是1，那么结果的第n为也为1，否则为0
 * 8. ~ : 非运算 操作数的第n位为1，那么结果的第n位为0，反之，也就是取反运算（一元操作符：只操作一个数）
 */
public class Times33 {
    public static void main(String[] args) {
        System.out.println(time33("zhouliang"));

    }

    static int time33(String str) {
        int len = str.length();
        int hash = 0;
        for (int i = 0; i < len; i++){
            // (hash << 5) + hash 相当于 hash * 33
            hash = (hash << 5) + hash + (int) str.charAt(i);
        }
        return hash;
    }
}
