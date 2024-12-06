package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;

public class POJO {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<MyCar> input = env.fromElements(
                new MyCar("BMW", 3000),
                new MyCar("Tesla", 4000),
                new MyCar("Tesla", 400),
                new MyCar("Rolls-Royce", 200)

        );
        final FilterOperator<MyCar> output= input.filter(new FilterFunction<MyCar>() {
            @Override
            public boolean filter(MyCar myCar) throws Exception {
                return myCar.amount > 1000;
            }
        });

        output.print();

    }
    public static class MyCar{
        public String brand;
        public int amount;
        public MyCar() {

        }
        public MyCar(String brand, int amount){
            this.brand = brand;
            this.amount = amount;

        }
        @Override
        public  String toString(){
            return "MyCar{" + "brand='" + +'\'' + ", amount=" + amount+ '}';
        }
    }

}


