package com.ljy.study.hadoop.mapreduce.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装reduce的输出类型，用于输出一个电话对应的上行流量、下行流量和总流量
 *适合一次在reduce输出多类数据
 */
// 1 实现writable接口，才能用于reduce输出
public class FlowBean implements Writable {
    private long upFlow;//上行流量
    private long downFlow;//下行流量
    private long sumFlow;//统计上行流量和下行流量的综合

    //2  反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;//实现流量相加
    }

    //3  写序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    //4 反序列化方法
    //5 反序列化方法读顺序必须和写序列化方法的写顺序必须一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow  = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    // 6 编写toString方法，方便后续打印到文本
    @Override
    public String toString() {
        //return upFlow + "\t" + downFlow + "\t" + sumFlow;
        return upFlow + "+" + downFlow + "=" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
