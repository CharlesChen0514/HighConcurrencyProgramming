package org.bitkernel;

import org.springframework.util.StopWatch;

public class TaskExecutor {
    public static long myPow(long x, int n) {
        long N = n;
        return N >= 0 ? quickMul(x, N) : 1L / quickMul(x, -N);
    }

    public static long quickMul(long x, long N) {
        long ans = 1L;
        // 贡献的初始值为 x
        long x_contribute = x;
        // 在对 N 进行二进制拆分的同时计算答案
        while (N > 0) {
            if (N % 2 == 1) {
                // 如果 N 二进制表示的最低位为 1，那么需要计入贡献
                ans *= x_contribute;
            }
            // 将贡献不断地平方
            x_contribute *= x_contribute;
            // 舍弃 N 二进制表示的最低位，这样我们每次只要判断最低位即可
            N /= 2;
        }
        return ans;
    }

    public static void main(String[] args) {
        StopWatch stop = new StopWatch();
        stop.start();
        for (int i = 0; i < 100000000; i++) {
            double v = myPow(65535, 65535);
        }
        stop.stop();
        System.out.println(stop.getTotalTimeMillis());
    }
}
