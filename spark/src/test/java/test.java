import com.alibaba.fastjson.JSON;

import java.util.List;

public class test {
    public static void main(String[] args) {
        String json = "{\"id\":1,\"name\":\"zs\",\"sources\":[{\"id\":1,\"name\":\"java\",\"score\":80.00},{\"id\":2,\"name\":\"python\",\"score\":90.00},{\"id\":3,\"name\":\"hive\",\"score\":85.00}]}";
        Student student = JSON.parseObject(json, Student.class);
        System.out.println("Name: " + student.getName()); // 应输出 "zs"
        System.out.println("Sources size: " + student.getSources().size()); // 应输出 3
    }

    static class Student {
        private int id;       // JSON 中的 "id" → 对应 setId()
        private String name;  // JSON 中的 "name" → 对应 setName()
        private List<Course> sources;

        // Getters 和 Setters（必须严格遵循 JavaBean 规范）
        public int getId() { return id; }    // 修正：将 getID() 改为 getId()
        public void setId(int id) { this.id = id; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public List<Course> getSources() { return sources; }
        public void setSources(List<Course> sources) { this.sources = sources; }
    }

    static class Course {
        private int id;
        private String name;
        private double score;

        public int getId() {return id;}
        public void setId(int id) {this.id = id;}

        public String getName() {return name;}
        public void setName(String name) {this.name = name;}

        public double getScore() {return score;}
        public void setScore(double score) {this.score = score;}
    }
}
