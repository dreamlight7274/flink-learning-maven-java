package org.apache.flink;


public class User {
    private String name;
    private String gender;
    private Integer age;
    public User() {

    }
    public User(String name, String gender, Integer age){
        this.name = name;
        this.gender = gender;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getAge(){
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
// 控制print出来的格式，使之更具有可读性
    @Override
    public String toString(){
        return "People{" + "name=" + name +
                ", gender='" + gender+
                '\'' + ", age=" + age +
                '}';
    }
}
